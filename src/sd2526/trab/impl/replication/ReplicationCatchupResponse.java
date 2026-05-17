package sd2526.trab.impl.replication;

import java.util.ArrayList;
import java.util.List;

public class ReplicationCatchupResponse {

	private List<ReplicationOperation> operations = new ArrayList<>();

	public ReplicationCatchupResponse() {
	}

	public ReplicationCatchupResponse(List<ReplicationOperation> operations) {
		this.operations = operations;
	}

	public List<ReplicationOperation> getOperations() {
		return operations;
	}

	public void setOperations(List<ReplicationOperation> operations) {
		this.operations = operations;
	}
}
