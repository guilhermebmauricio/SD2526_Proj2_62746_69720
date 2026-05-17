package sd2526.trab.impl.java.servers;

import static sd2526.trab.api.java.Result.error;
import static sd2526.trab.api.java.Result.ok;

import java.net.URI;

import java.util.List;
import java.util.logging.Logger;

import sd2526.trab.api.Message;
import sd2526.trab.api.java.Messages;
import sd2526.trab.api.java.Result;
import sd2526.trab.api.java.Result.ErrorCode;
import sd2526.trab.impl.api.java.AdminMessages;
import sd2526.trab.impl.db.DB;
import sd2526.trab.impl.java.clients.Clients;
import sd2526.trab.impl.replication.OperationType;
import sd2526.trab.impl.replication.ReplicaRole;
import sd2526.trab.impl.replication.ReplicationAck;
import sd2526.trab.impl.replication.ReplicationCatchupResponse;
import sd2526.trab.impl.replication.ReplicationManager;
import sd2526.trab.impl.replication.ReplicationOperation;

public class JavaReplicatedMessagesService implements Messages, AdminMessages {

	private static final Logger Log = Logger.getLogger(JavaReplicatedMessagesService.class.getName());
	private static final int QUORUM_ACKS = 1;

	private final JavaMessages delegate;
	private final ReplicationManager replication;

	public JavaReplicatedMessagesService(ReplicaRole role, String serverUri, int logSize, int queueSize) {
		this.delegate = JavaMessages.getInstance();
		this.replication = new ReplicationManager(role, serverUri, logSize, queueSize);
	}

	public boolean isPrimary() {
		return replication.isPrimary();
	}

	public long currentVersion() {
		return replication.currentVersion();
	}

	public URI primaryUri() {
		return replication.primaryUri();
	}

	public boolean shouldRedirectRead(Long expectedVersion) {
		return !replication.isPrimary()
				&& expectedVersion != null
				&& replication.currentVersion() < expectedVersion;
	}

	public void triggerCatchUpToVersion(long expectedVersion) {
		replication.triggerCatchUpAsync(expectedVersion, this::applyReplicatedOperation);
	}

	@Override
	public Result<String> postMessage(String pwd, Message msg) {
		if (!replication.isPrimary()) {
			return error(ErrorCode.FORBIDDEN);
		}

		var res = delegate.postMessage(pwd, msg);
		if (!res.isOK()) {
			return res;
		}

		String mid = res.value();
		var stored = DB.getOne(mid, Message.class);
		if (!stored.isOK()) {
			return error(stored);
		}

		var op = new ReplicationOperation();
		op.setType(OperationType.POST_MESSAGE);
		op.setMessage(stored.value());

		var replicate = replicateFromPrimary(op);
		if (!replicate.isOK()) {
			return error(replicate);
		}

		return ok(mid);
	}

	@Override
	public Result<Message> getInboxMessage(String name, String mid, String pwd) {
		return delegate.getInboxMessage(name, mid, pwd);
	}

	@Override
	public Result<List<String>> getAllInboxMessages(String name, String pwd) {
		return delegate.getAllInboxMessages(name, pwd);
	}

	@Override
	public Result<Void> removeInboxMessage(String name, String mid, String pwd) {
		if (!replication.isPrimary()) {
			return error(ErrorCode.FORBIDDEN);
		}

		var res = delegate.removeInboxMessage(name, mid, pwd);
		if (!res.isOK()) {
			return res;
		}

		var op = new ReplicationOperation();
		op.setType(OperationType.REMOVE_INBOX_MESSAGE);
		op.setName(name);
		op.setMid(mid);

		return replicateFromPrimary(op);
	}

	@Override
	public Result<Void> deleteMessage(String name, String mid, String pwd) {
		if (!replication.isPrimary()) {
			return error(ErrorCode.FORBIDDEN);
		}

		var res = delegate.deleteMessage(name, mid, pwd);
		if (!res.isOK()) {
			return res;
		}

		var op = new ReplicationOperation();
		op.setType(OperationType.DELETE_MESSAGE);
		op.setMid(mid);

		return replicateFromPrimary(op);
	}

	@Override
	public Result<List<String>> searchInbox(String name, String pwd, String query) {
		return delegate.searchInbox(name, pwd, query);
	}

	@Override
	public Result<Void> remotePostMessage(Message m) {
		if (!replication.isPrimary()) {
			return error(ErrorCode.FORBIDDEN);
		}

		var res = delegate.remotePostMessage(m);
		if (!res.isOK()) {
			return res;
		}

		var op = new ReplicationOperation();
		op.setType(OperationType.REMOTE_POST_MESSAGE);
		op.setMessage(m);

		return replicateFromPrimary(op);
	}

	@Override
	public Result<Void> remoteDeleteMessage(String mid) {
		if (!replication.isPrimary()) {
			return error(ErrorCode.FORBIDDEN);
		}

		var res = delegate.remoteDeleteMessage(mid);
		if (!res.isOK()) {
			return res;
		}

		var op = new ReplicationOperation();
		op.setType(OperationType.REMOTE_DELETE_MESSAGE);
		op.setMid(mid);

		return replicateFromPrimary(op);
	}

	@Override
	public Result<Void> remoteDeleteUserInbox(String name) {
		if (!replication.isPrimary()) {
			return error(ErrorCode.FORBIDDEN);
		}

		var res = delegate.remoteDeleteUserInbox(name);
		if (!res.isOK()) {
			return res;
		}

		var op = new ReplicationOperation();
		op.setType(OperationType.REMOTE_DELETE_USER_INBOX);
		op.setName(name);

		return replicateFromPrimary(op);
	}

	@Override
	public Result<ReplicationAck> replicateOperation(ReplicationOperation op) {
		if (replication.isPrimary()) {
			return error(ErrorCode.FORBIDDEN);
		}

		return replication.onReplicatedOperation(op, this::applyReplicatedOperation);
	}

	@Override
	public Result<ReplicationCatchupResponse> getOperationsAfter(long seq, int limit) {
		if (!replication.isPrimary()) {
			return primaryAdmin().getOperationsAfter(seq, limit);
		}
		return replication.getOperationsAfter(seq, limit);
	}

	private Result<Void> replicateFromPrimary(ReplicationOperation op) {
		if (!replication.isPrimary()) {
			return error(ErrorCode.FORBIDDEN);
		}

		replication.registerPrimaryOperation(op);
		return replication.replicateToSecondaries(op, QUORUM_ACKS);
	}

	private Result<Void> applyReplicatedOperation(ReplicationOperation op) {
		return switch (op.getType()) {
			case POST_MESSAGE, REMOTE_POST_MESSAGE -> delegate.remotePostMessage(op.getMessage());
			case DELETE_MESSAGE, REMOTE_DELETE_MESSAGE -> delegate.remoteDeleteMessage(op.getMid());
			case REMOVE_INBOX_MESSAGE -> removeInboxLocally(op.getName(), op.getMid());
			case REMOTE_DELETE_USER_INBOX -> delegate.remoteDeleteUserInbox(op.getName());
		};
	}

	private Result<Void> removeInboxLocally(String name, String mid) {
		var res = DB.deleteOne(new InboxEntry(mid, name)).mapToVoid();
		if (res.isOK() || res.error() == ErrorCode.NOT_FOUND) {
			return ok();
		}
		return res;
	}

	private AdminMessages primaryAdmin() {
		return Clients.AdminMessagesClient.get(replication.primaryUri());
	}

	public static ReplicaRole parseRole(String[] args) {
		for (var arg : args) {
			if ("primary".equalsIgnoreCase(arg)) {
				return ReplicaRole.PRIMARY;
			}
			if ("secondary".equalsIgnoreCase(arg)) {
				return ReplicaRole.SECONDARY;
			}
		}
		return ReplicaRole.SECONDARY;
	}

	public static int parseIntArg(String[] args, String key, int defaultValue) {
		for (int i = 0; i < args.length - 1; i++) {
			if (key.equalsIgnoreCase(args[i])) {
				try {
					return Integer.parseInt(args[i + 1]);
				} catch (NumberFormatException ignored) {
					return defaultValue;
				}
			}
		}
		return defaultValue;
	}
}



