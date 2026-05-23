
package sd2526.trab.impl.api.java;

import sd2526.trab.api.java.Result;
import sd2526.trab.impl.replication.ReplicationAck;
import sd2526.trab.impl.replication.ReplicationCatchupResponse;
import sd2526.trab.impl.replication.ReplicationOperation;

public interface ReplicationMessages {

	Result<Long> getCurrentVersion();

	Result<ReplicationAck> replicateOperation(ReplicationOperation op);

	Result<ReplicationCatchupResponse> getOperationsAfter(long seq, int limit);
}
