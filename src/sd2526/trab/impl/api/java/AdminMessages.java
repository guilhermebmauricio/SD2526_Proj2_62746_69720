package sd2526.trab.impl.api.java;

import sd2526.trab.api.Message;
import sd2526.trab.api.java.Result;
import sd2526.trab.impl.replication.ReplicationAck;
import sd2526.trab.impl.replication.ReplicationCatchupResponse;
import sd2526.trab.impl.replication.ReplicationOperation;

public interface AdminMessages {

	Result<Void> remotePostMessage(Message m);

	Result<Void> remoteDeleteMessage(String mid);

	Result<Void> remoteDeleteUserInbox(String name);

	Result<Long> getCurrentVersion();

	Result<ReplicationAck> replicateOperation(ReplicationOperation op);

	Result<ReplicationCatchupResponse> getOperationsAfter(long seq, int limit);
}
