package sd2526.trab.impl.rest.clients;

import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.core.GenericType;
import jakarta.ws.rs.core.MediaType;
import sd2526.trab.impl.rest.servers.ServerSecretFilter;
import sd2526.trab.impl.utils.ServerSecret;
import sd2526.trab.api.java.Result;
import sd2526.trab.api.rest.RestMessages;
import sd2526.trab.impl.api.java.ReplicationMessages;
import sd2526.trab.impl.api.rest.RestReplicationMessages;
import sd2526.trab.impl.replication.ReplicationAck;
import sd2526.trab.impl.replication.ReplicationCatchupResponse;
import sd2526.trab.impl.replication.ReplicationOperation;

public class RestReplicationMessagesClient extends RestClient implements ReplicationMessages {

	public RestReplicationMessagesClient(String serverURI) {
		super(serverURI, RestMessages.PATH);
	}

	@Override
	public Result<Long> getCurrentVersion() {
		return super.reTry(() -> doGetCurrentVersion());
	}

	@Override
	public Result<ReplicationAck> replicateOperation(ReplicationOperation op) {
		return super.reTry(() -> doReplicateOperation(op));
	}

	@Override
	public Result<ReplicationCatchupResponse> getOperationsAfter(long seq, int limit) {
		return super.reTry(() -> doGetOperationsAfter(seq, limit));
	}

	private Result<Long> doGetCurrentVersion() {
		return super.toJavaResult(target
				.path(RestReplicationMessages.REPLICATION)
				.path(RestReplicationMessages.VERSION)
				.request()
				.header(ServerSecretFilter.HEADER, ServerSecret.get())
				.accept(MediaType.APPLICATION_JSON)
				.get(), Long.class);
	}

	private Result<ReplicationAck> doReplicateOperation(ReplicationOperation op) {
		return super.toJavaResult(target
				.path(RestReplicationMessages.REPLICATION)
				.request()
				.header(ServerSecretFilter.HEADER, ServerSecret.get())
				.accept(MediaType.APPLICATION_JSON)
				.post(Entity.entity(op, MediaType.APPLICATION_JSON)), ReplicationAck.class);
	}

	private Result<ReplicationCatchupResponse> doGetOperationsAfter(long seq, int limit) {
		return super.toJavaResult(target
				.path(RestReplicationMessages.REPLICATION)
				.queryParam(RestReplicationMessages.AFTER, seq)
				.queryParam(RestReplicationMessages.LIMIT, limit)
				.request()
				.header(ServerSecretFilter.HEADER, ServerSecret.get())
				.accept(MediaType.APPLICATION_JSON)
				.get(), new GenericType<ReplicationCatchupResponse>() {});
	}
}
