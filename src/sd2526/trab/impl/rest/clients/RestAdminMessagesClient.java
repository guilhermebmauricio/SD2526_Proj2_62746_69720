package sd2526.trab.impl.rest.clients;

import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.core.GenericType;
import jakarta.ws.rs.core.MediaType;
import sd2526.trab.api.Message;
import sd2526.trab.api.java.Result;
import sd2526.trab.api.rest.RestMessages;
import sd2526.trab.impl.api.java.AdminMessages;
import sd2526.trab.impl.api.rest.RestAdminMessages;
import sd2526.trab.impl.replication.ReplicationAck;
import sd2526.trab.impl.replication.ReplicationCatchupResponse;
import sd2526.trab.impl.replication.ReplicationOperation;

public class RestAdminMessagesClient extends RestClient implements AdminMessages {

	public RestAdminMessagesClient(String serverURI) {
		super(serverURI, RestMessages.PATH);
	}

	@Override
	public Result<Void> remotePostMessage(Message m) {
		return super.reTry( () -> doRemotePostMessage(m) );
	}

	@Override
	public Result<Void> remoteDeleteMessage(String mid) {
		return super.reTry( () -> doRemoteDeleteMessage(mid) );
	}

	@Override
	public Result<Void> remoteDeleteUserInbox(String name) {
		return super.reTry( () -> doRemoteDeleteUserInbox(name) );
	}

	@Override
	public Result<ReplicationAck> replicateOperation(ReplicationOperation op) {
		return super.reTry(() -> doReplicateOperation(op));
	}

	@Override
	public Result<ReplicationCatchupResponse> getOperationsAfter(long seq, int limit) {
		return super.reTry(() -> doGetOperationsAfter(seq, limit));
	}
	
	private Result<Void> doRemotePostMessage(Message msg) {
		return super.toJavaResult( target
				.path(RestAdminMessages.ADMIN)
				.request()
				.post( Entity.entity(msg, MediaType.APPLICATION_JSON )));
	}

	private Result<Void> doRemoteDeleteMessage(String mid) {
		return super.toJavaResult( target
				.path(RestAdminMessages.ADMIN)
				.path( mid )
				.request()
				.delete());
	}
	
	private Result<Void> doRemoteDeleteUserInbox(String name) {
		return super.toJavaResult( target
				.path(RestAdminMessages.ADMIN)
				.path(RestAdminMessages.INBOX)
				.path( name )
				.request()
				.delete());
	}

	private Result<ReplicationAck> doReplicateOperation(ReplicationOperation op) {
		return super.toJavaResult(target
				.path(RestAdminMessages.ADMIN)
				.path(RestAdminMessages.REPLICATION)
				.request()
				.accept(MediaType.APPLICATION_JSON)
				.post(Entity.entity(op, MediaType.APPLICATION_JSON)), ReplicationAck.class);
	}

	private Result<ReplicationCatchupResponse> doGetOperationsAfter(long seq, int limit) {
		return super.toJavaResult(target
				.path(RestAdminMessages.ADMIN)
				.path(RestAdminMessages.REPLICATION)
				.queryParam(RestAdminMessages.AFTER, seq)
				.queryParam(RestAdminMessages.LIMIT, limit)
				.request()
				.accept(MediaType.APPLICATION_JSON)
				.get(), new GenericType<ReplicationCatchupResponse>() {});
	}
}
