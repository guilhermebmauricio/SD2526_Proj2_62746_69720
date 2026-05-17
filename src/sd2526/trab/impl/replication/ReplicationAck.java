package sd2526.trab.impl.replication;

public class ReplicationAck {

	private long seq;
	private String replicaId;

	public ReplicationAck() {
	}

	public ReplicationAck(long seq, String replicaId) {
		this.seq = seq;
		this.replicaId = replicaId;
	}

	public long getSeq() {
		return seq;
	}

	public void setSeq(long seq) {
		this.seq = seq;
	}

	public String getReplicaId() {
		return replicaId;
	}

	public void setReplicaId(String replicaId) {
		this.replicaId = replicaId;
	}
}
