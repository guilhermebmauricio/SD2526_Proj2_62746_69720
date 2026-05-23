package sd2526.trab.impl.replication;

import static sd2526.trab.api.java.Result.error;
import static sd2526.trab.api.java.Result.ok;

import java.net.URI;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Deque;
import java.util.List;
import java.util.PriorityQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import sd2526.trab.api.java.Messages;
import sd2526.trab.api.java.Result;
import sd2526.trab.api.java.Result.ErrorCode;
import sd2526.trab.impl.discovery.Discovery;
import sd2526.trab.impl.java.clients.Clients;
import sd2526.trab.impl.utils.IP;
import sd2526.trab.impl.replication.LeadershipListener;
import sd2526.trab.impl.replication.LeaderElection; 

public class ReplicationManager implements LeadershipListener {

	public interface OperationApplier {
		Result<Void> apply(ReplicationOperation op);
	}

	private static final Logger Log = Logger.getLogger(ReplicationManager.class.getName());

	private ReplicaRole role;
	private final URI selfUri;
	private final int maxLogSize;
	private final int maxPendingSize;

	private final Deque<ReplicationOperation> operationLog = new ArrayDeque<>();
	private final PriorityQueue<ReplicationOperation> pendingOps =
			new PriorityQueue<>(Comparator.comparingLong(ReplicationOperation::getSeq));
	private static final long REPLICATION_TIMEOUT_MS = 5_000L;

	private final ExecutorService catchupExecutor = Executors.newSingleThreadExecutor(r -> {
		var thread = new Thread(r);
		thread.setDaemon(true);
		thread.setName("messages-replication-catchup");
		return thread;
	});

	private final ExecutorService replicationExecutor = Executors.newCachedThreadPool(r -> {
		var thread = new Thread(r);
		thread.setDaemon(true);
		thread.setName("messages-replication-fanout");
		return thread;
	});

	private long sequenceCounter;
	private long lastAppliedSeq;
	private boolean catchupInFlight;
	private long catchupTargetSeq;
	private URI catchupSourceUri;
	private URI leaderUri;
	private boolean writeEnabled;
	private long leadershipEpoch;

    private final LeaderElection leaderElection;
    private OperationApplier promotionApplier;


	public ReplicationManager(String serviceName, String selfUri, String zookeeperAddress, int maxLogSize, int maxPendingSize) {
		this.role = ReplicaRole.UNKNOWN;
		this.selfUri = URI.create(selfUri);
		this.maxLogSize = Math.max(10, maxLogSize);
		this.maxPendingSize = Math.max(10, maxPendingSize);
		this.sequenceCounter = 0L;
		this.lastAppliedSeq = 0L;
		this.catchupSourceUri = null;
		this.leaderUri = null;
		this.writeEnabled = false;
		this.leadershipEpoch = 0L;
		this.leaderElection = new LeaderElection(zookeeperAddress, serviceName, this);
	}

    public void setPromotionApplier(OperationApplier applier) {
        this.promotionApplier = applier;
    }

    public void start() {
        leaderElection.start();
		synchronized (this) {
			this.role = leaderElection.isLeader() ? ReplicaRole.PRIMARY : ReplicaRole.SECONDARY;
			this.writeEnabled = this.role == ReplicaRole.PRIMARY;
			this.leaderUri = this.role == ReplicaRole.PRIMARY ? selfUri : leaderUriFromHost(leaderElection.getLeaderHost());
		}
    }

    @Override
    public void onLeadershipChange(boolean isLeader, String leaderZNode, String leaderHost) {
		boolean promoted;
		synchronized (this) {
			ReplicaRole oldRole = this.role;
			leadershipEpoch++;
			catchupInFlight = false;
			catchupTargetSeq = currentVersionLocked();
			catchupSourceUri = null;

			if (isLeader) {
				Log.info("This replica is now the primary.");
				this.role = ReplicaRole.PRIMARY;
				this.leaderUri = selfUri;
				this.writeEnabled = oldRole == ReplicaRole.PRIMARY || this.writeEnabled;
			} else {
				Log.info("This replica is now a secondary. Current leader: " + leaderHost);
				this.role = ReplicaRole.SECONDARY;
				this.leaderUri = leaderUriFromHost(leaderHost);
				this.writeEnabled = false;
			}

			promoted = oldRole != ReplicaRole.PRIMARY && this.role == ReplicaRole.PRIMARY;
		}

		if (promoted) {
			Log.info("Primary promotion entered sync phase; writes remain disabled until catch-up completes.");
			triggerCatchUpToLatestKnownVersion();
		}
    }

    public boolean isSyncing() {
        return catchupInFlight;
    }

	public boolean isPrimary() {
		return role == ReplicaRole.PRIMARY;
	}

	public synchronized boolean isWritablePrimary() {
		return role == ReplicaRole.PRIMARY && writeEnabled;
	}

	public synchronized long lastAppliedSeq() {
		return lastAppliedSeq;
	}

	public synchronized long currentVersion() {
		return Math.max(sequenceCounter, lastAppliedSeq);
	}

	public synchronized long registerPrimaryOperation(ReplicationOperation op) {
		if (!isWritablePrimary()) {
			throw new IllegalStateException("Only the primary can allocate sequence numbers.");
		}

		op.setSeq(++sequenceCounter);
		appendToLog(op);
		return op.getSeq();
	}

	public synchronized Result<ReplicationCatchupResponse> getOperationsAfter(long seq, int limit) {
		int max = Math.max(1, Math.min(limit, maxLogSize));
        // very Rust-like
		var ops = operationLog.stream()
				.filter(op -> op.getSeq() > seq)
				.sorted(Comparator.comparingLong(ReplicationOperation::getSeq))
				.limit(max)
				.collect(Collectors.toList());
		return ok(new ReplicationCatchupResponse(ops));
	}

	public Result<Void> replicateToSecondaries(ReplicationOperation op, int quorumAcks) {
		if (!isPrimary()) {
			return error(ErrorCode.FORBIDDEN);
		}

		var secondaries = discoverPeerReplicas().stream()
				.filter(uri -> !uri.equals(selfUri))
				.collect(Collectors.toList());

		if (quorumAcks <= 0) return ok();
		if (secondaries.size() < quorumAcks) return error(ErrorCode.TIMEOUT);

		var ackCount  = new AtomicInteger(0);
		var doneCount = new AtomicInteger(0);
		var quorumFuture = new CompletableFuture<Result<Void>>();
		int total = secondaries.size();

        // for loop + executor + future allow for parallel replication to secondaries
		for (var secondary : secondaries) {
            // execute and return immediately, allowing other replications to proceed in parallel
			replicationExecutor.execute(() -> {
			var res = Clients.ReplicationMessagesClient.get(secondary).replicateOperation(op);
				boolean succeeded = res.isOK() && res.value() != null && res.value().getSeq() >= op.getSeq();

				if (!succeeded) {
					Log.info(() -> "Replication to %s failed for seq=%d".formatted(secondary, op.getSeq()));
				}

				int acks = succeeded ? ackCount.incrementAndGet() : ackCount.get();
				int done = doneCount.incrementAndGet();

                // releases the future based on the total result
				if (acks >= quorumAcks) {
					quorumFuture.complete(ok());
				} else if (done == total) {
					quorumFuture.complete(error(ErrorCode.TIMEOUT));
				}
			});
		}

		try {
			return quorumFuture.get(REPLICATION_TIMEOUT_MS, TimeUnit.MILLISECONDS); // blocked until future released
		} catch (TimeoutException e) {
			return error(ErrorCode.TIMEOUT);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			return error(ErrorCode.INTERNAL_ERROR);
		} catch (java.util.concurrent.ExecutionException e) {
			return error(ErrorCode.INTERNAL_ERROR);
		}
	}

	public Result<ReplicationAck> onReplicatedOperation(ReplicationOperation op, OperationApplier applier) {
		synchronized (this) {
			if (op.getSeq() <= lastAppliedSeq) {
				return ok(new ReplicationAck(op.getSeq(), selfUri.toString()));
			}

			if (op.getSeq() > lastAppliedSeq + 1) {
				if (pendingOps.size() >= maxPendingSize) {
					return error(ErrorCode.CONFLICT);
				}
				pendingOps.offer(op);

				var catchup = catchUpMissingLocked(primaryUri(), applier);
				if (!catchup.isOK()) {
					return error(catchup);
				}
			}

			if (op.getSeq() == lastAppliedSeq + 1) {
				var apply = applyLocked(op, applier);
				if (!apply.isOK()) {
					return error(apply);
				}
			}

			var drain = drainPendingLocked(applier);
			if (!drain.isOK()) {
				return error(drain);
			}

			if (lastAppliedSeq >= op.getSeq()) {
				return ok(new ReplicationAck(op.getSeq(), selfUri.toString()));
			}

			return error(ErrorCode.TIMEOUT);
		}
	}

	public synchronized URI primaryUri() {
		if (isPrimary()) {
			return selfUri;
		}
		if (leaderUri == null) {
			leaderUri = leaderUriFromHost(leaderElection.getLeaderHost());
		}
		if (leaderUri == null) {
			throw new IllegalStateException("Leader URI unavailable from ZooKeeper state.");
		}
		return leaderUri;
	}

	private void triggerCatchUpToLatestKnownVersion() {
		long localVersion = currentVersion();
		long bestVersion = localVersion;
		URI bestSource = null;

		for (var peer : discoverPeerReplicas()) {
			if (peer.equals(selfUri)) {
				continue;
			}

		var res = Clients.ReplicationMessagesClient.get(peer).getCurrentVersion();
			if (res.isOK() && res.value() != null && res.value() > bestVersion) {
				bestVersion = res.value();
				bestSource = peer;
			}
		}

		OperationApplier applier = promotionApplier != null ? promotionApplier : op -> ok();
		triggerCatchUpAsync(bestVersion, bestSource, applier);
	}

	public void triggerCatchUpAsync(long targetVersion, URI sourceUri, OperationApplier applier) {
		long runEpoch;
		synchronized (this) {
			if (targetVersion <= currentVersionLocked()) {
				if (role == ReplicaRole.PRIMARY) {
					writeEnabled = true;
				}
				return;
			}

			if (sourceUri == null) {
				Log.info(() -> "Catch-up source is unknown while targeting version %d".formatted(targetVersion));
				return;
			}

			catchupTargetSeq = Math.max(catchupTargetSeq, targetVersion);
			catchupSourceUri = sourceUri;
			if (catchupInFlight) {
				return;
			}

			catchupInFlight = true;
			runEpoch = leadershipEpoch;
		}

		catchupExecutor.execute(() -> runCatchUpLoop(applier, runEpoch));
	}

	private synchronized Result<Void> catchUpMissingLocked(URI sourceUri, OperationApplier applier) {
		var res = Clients.ReplicationMessagesClient.get(sourceUri).getOperationsAfter(lastAppliedSeq, maxLogSize);
		if (!res.isOK() || res.value() == null) {
			return error(res);
		}

		var ops = new ArrayList<>(res.value().getOperations());
		ops.sort(Comparator.comparingLong(ReplicationOperation::getSeq));

		for (var op : ops) {
			if (op.getSeq() <= lastAppliedSeq) {
				continue;
			}
			if (op.getSeq() > lastAppliedSeq + 1) {
				if (pendingOps.size() >= maxPendingSize) {
					return error(ErrorCode.CONFLICT);
				}
				pendingOps.offer(op);
				break;
			}

			var apply = applyLocked(op, applier);
			if (!apply.isOK()) {
				return apply;
			}
		}

		return drainPendingLocked(applier);
	}

	private Result<Void> drainPendingLocked(OperationApplier applier) {
		while (!pendingOps.isEmpty() && pendingOps.peek().getSeq() == lastAppliedSeq + 1) {
			var next = pendingOps.poll();
			var apply = applyLocked(next, applier);
			if (!apply.isOK()) {
				return apply;
			}
		}
		return ok();
	}

	private Result<Void> applyLocked(ReplicationOperation op, OperationApplier applier) {
		var res = applier.apply(op);
		if (!res.isOK()) {
			return res;
		}

		appendToLog(op);
		lastAppliedSeq = Math.max(lastAppliedSeq, op.getSeq());
		sequenceCounter = Math.max(sequenceCounter, op.getSeq());
		return ok();
	}

	private void runCatchUpLoop(OperationApplier applier, long runEpoch) {
		while (true) {
			long beforeVersion;
			long targetVersion;
			URI sourceUri;

			synchronized (this) {
				if (runEpoch != leadershipEpoch || !catchupInFlight) {
					return;
				}
				beforeVersion = currentVersionLocked();
				targetVersion = catchupTargetSeq;
				sourceUri = catchupSourceUri;
				if (beforeVersion >= targetVersion) {
					catchupInFlight = false;
					catchupSourceUri = null;
					if (role == ReplicaRole.PRIMARY) {
						writeEnabled = true;
					}
					return;
				}
			}

			if (sourceUri == null) {
				Log.info(() -> "Catch-up source vanished while targeting version %d".formatted(targetVersion));
				synchronized (this) {
					if (runEpoch == leadershipEpoch) {
						catchupInFlight = false;
						if (role == ReplicaRole.PRIMARY) {
							writeEnabled = true;
						}
					}
				}
				return;
			}

			var catchup = catchUpMissingLocked(sourceUri, applier);
			if (!catchup.isOK()) {
				Log.info(() -> "Catch-up failed while targeting version %d".formatted(targetVersion));
				synchronized (this) {
					if (runEpoch == leadershipEpoch) {
						catchupInFlight = false;
						catchupSourceUri = null;
						if (role == ReplicaRole.PRIMARY) {
							writeEnabled = true;
						}
					}
				}
				return;
			}

			synchronized (this) {
				if (runEpoch != leadershipEpoch || !catchupInFlight) {
					return;
				}
				long afterVersion = currentVersionLocked();
				if (afterVersion >= catchupTargetSeq) {
					catchupInFlight = false;
					catchupSourceUri = null;
					if (role == ReplicaRole.PRIMARY) {
						writeEnabled = true;
					}
					return;
				}
				if (afterVersion <= beforeVersion) {
					Log.info(() -> "Catch-up stalled at version %d while targeting %d"
							.formatted(afterVersion, catchupTargetSeq));
					catchupInFlight = false;
					catchupSourceUri = null;
					if (role == ReplicaRole.PRIMARY) {
						writeEnabled = true;
					}
					return;
				}
			}
		}
	}

	private long currentVersionLocked() {
		return Math.max(sequenceCounter, lastAppliedSeq);
	}

	private void appendToLog(ReplicationOperation op) {
		operationLog.addLast(copy(op));
		while (operationLog.size() > maxLogSize) {
			operationLog.removeFirst();
		}
	}

	private List<URI> discoverPeerReplicas() {
		var sn = "%s@%s".formatted(Messages.SERVICE_NAME, IP.domain());
		return Arrays.stream(Discovery.getInstance().knownUrisOf(sn, 1)).toList();
	}

	private static ReplicationOperation copy(ReplicationOperation op) {
		var c = new ReplicationOperation();
		c.setSeq(op.getSeq());
		c.setType(op.getType());
		c.setSender(op.getSender());
		c.setPwd(op.getPwd());
		c.setName(op.getName());
		c.setMid(op.getMid());
		c.setMessage(op.getMessage() == null ? null : new sd2526.trab.api.Message(op.getMessage()));
		return c;
	}

	private URI leaderUriFromHost(String host) {
		if (host == null || host.isBlank()) {
			return null;
		}

		if (host.equalsIgnoreCase(selfUri.getHost())) {
			return selfUri;
		}

		try {
			return new URI(selfUri.getScheme(), selfUri.getUserInfo(), host, selfUri.getPort(), selfUri.getPath(), null, null);
		} catch (Exception e) {
			Log.info(() -> "Failed to build leader URI from host '%s': %s".formatted(host, e.getMessage()));
			return null;
		}
	}
}
