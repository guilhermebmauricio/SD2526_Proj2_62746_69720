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
import java.util.logging.Logger;
import java.util.stream.Collectors;

import sd2526.trab.api.java.Messages;
import sd2526.trab.api.java.Result;
import sd2526.trab.api.java.Result.ErrorCode;
import sd2526.trab.impl.discovery.Discovery;
import sd2526.trab.impl.java.clients.Clients;
import sd2526.trab.impl.utils.IP;

public class ReplicationManager {

	public interface OperationApplier {
		Result<Void> apply(ReplicationOperation op);
	}

	private static final Logger Log = Logger.getLogger(ReplicationManager.class.getName());
	private static final String PRIMARY_SERVICE = "MessagesPrimary";

	private final ReplicaRole role;
	private final URI selfUri;
	private final int maxLogSize;
	private final int maxPendingSize;

	private final Deque<ReplicationOperation> operationLog = new ArrayDeque<>();
	private final PriorityQueue<ReplicationOperation> pendingOps =
			new PriorityQueue<>(Comparator.comparingLong(ReplicationOperation::getSeq));

	private long sequenceCounter;
	private long lastAppliedSeq;

	public ReplicationManager(ReplicaRole role, String selfUri, int maxLogSize, int maxPendingSize) {
		this.role = role;
		this.selfUri = URI.create(selfUri);
		this.maxLogSize = Math.max(10, maxLogSize);
		this.maxPendingSize = Math.max(10, maxPendingSize);
		this.sequenceCounter = 0L;
		this.lastAppliedSeq = 0L;

		if (isPrimary()) {
			var primaryServiceName = "%s@%s".formatted(PRIMARY_SERVICE, IP.domain());
			Discovery.getInstance().announce(primaryServiceName, selfUri);
		}
	}

	public boolean isPrimary() {
		return role == ReplicaRole.PRIMARY;
	}

	public synchronized long lastAppliedSeq() {
		return lastAppliedSeq;
	}

	public synchronized long registerPrimaryOperation(ReplicationOperation op) {
		if (!isPrimary()) {
			throw new IllegalStateException("Only the primary can allocate sequence numbers.");
		}

		op.setSeq(++sequenceCounter);
		appendToLog(op);
		return op.getSeq();
	}

	public synchronized Result<ReplicationCatchupResponse> getOperationsAfter(long seq, int limit) {
		int max = Math.max(1, Math.min(limit, maxLogSize));
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

		int ackCount = 0;
		for (var secondary : secondaries) {
			var res = Clients.AdminMessagesClient.get(secondary).replicateOperation(op);
			if (res.isOK() && res.value() != null && res.value().getSeq() >= op.getSeq()) {
				ackCount++;
				if (ackCount >= quorumAcks) {
					return ok();
				}
			} else {
				Log.info(() -> "Replication to %s failed for seq=%d".formatted(secondary, op.getSeq()));
			}
		}

		return error(ErrorCode.TIMEOUT);
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

				var catchup = catchUpMissingLocked(applier);
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

	public URI primaryUri() {
		if (isPrimary()) {
			return selfUri;
		}

		var primaryServiceName = "%s@%s".formatted(PRIMARY_SERVICE, IP.domain());
		return Discovery.getInstance().knownUrisOf(primaryServiceName, 1)[0];
	}

	private synchronized Result<Void> catchUpMissingLocked(OperationApplier applier) {
		URI primary = primaryUri();
		var res = Clients.AdminMessagesClient.get(primary).getOperationsAfter(lastAppliedSeq, maxLogSize);
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

		lastAppliedSeq = Math.max(lastAppliedSeq, op.getSeq());
		sequenceCounter = Math.max(sequenceCounter, op.getSeq());
		return ok();
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
}
