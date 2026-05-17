package sd2526.trab.impl.rest.servers;

import java.util.List;
import java.net.URI;

import jakarta.inject.Singleton;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.Provider;
import sd2526.trab.api.Message;
import sd2526.trab.api.java.Messages;
import sd2526.trab.api.rest.RestMessages;
import sd2526.trab.impl.api.java.AdminMessages;
import sd2526.trab.impl.api.rest.RestAdminMessages;
import sd2526.trab.impl.java.servers.JavaReplicatedMessagesService;
import sd2526.trab.impl.replication.ReplicationAck;
import sd2526.trab.impl.replication.ReplicationCatchupResponse;
import sd2526.trab.impl.replication.ReplicationOperation;

@Provider
@Singleton
public class RestReplicatedMessagesResource extends RestResource implements RestMessages, RestAdminMessages {

	private final Messages messages;
	private final AdminMessages admin;

	public RestReplicatedMessagesResource(Messages messages, AdminMessages admin) {
		this.messages = messages;
		this.admin = admin;
	}

	@Override
	public String postMessage(String pwd, Message msg) {
		redirectIfSecondary(buildPostMessageUri(pwd), true);
		return super.resultOrThrow(messages.postMessage(pwd, msg));
	}

	@Override
	public Message getMessage(String name, String mid, String pwd) {
		redirectIfSecondary(buildGetMessageUri(name, mid, pwd), false);
		return super.resultOrThrow(messages.getInboxMessage(name, mid, pwd));
	}

	@Override
	public List<String> getMessages(String name, String pwd, String query) {
		redirectIfSecondary(buildGetMessagesUri(name, pwd, query), false);
		if (query != null && !query.isEmpty()) {
			return super.resultOrThrow(messages.searchInbox(name, pwd, query));
		}
		return super.resultOrThrow(messages.getAllInboxMessages(name, pwd));
	}

	@Override
	public void removeFromUserInbox(String name, String mid, String pwd) {
		redirectIfSecondary(buildRemoveInboxUri(name, mid, pwd), false);
		super.resultOrThrow(messages.removeInboxMessage(name, mid, pwd));
	}

	@Override
	public void deleteMessage(String name, String mid, String pwd) {
		redirectIfSecondary(buildDeleteMessageUri(name, mid, pwd), false);
		super.resultOrThrow(messages.deleteMessage(name, mid, pwd));
	}

	@Override
	public void remotePostMessage(Message m) {
		super.resultOrThrow(admin.remotePostMessage(m));
	}

	@Override
	public void remoteDeleteMessage(String mid) {
		super.resultOrThrow(admin.remoteDeleteMessage(mid));
	}

	@Override
	public void remoteDeleteUserInbox(String name) {
		super.resultOrThrow(admin.remoteDeleteUserInbox(name));
	}

	@Override
	public ReplicationAck replicateOperation(ReplicationOperation op) {
		return super.resultOrThrow(admin.replicateOperation(op));
	}

	@Override
	public ReplicationCatchupResponse getOperationsAfter(long seq, int limit) {
		return super.resultOrThrow(admin.getOperationsAfter(seq, limit));
	}

	private void redirectIfSecondary(URI targetUri, boolean preserveMethodBody) {
		if (messages instanceof JavaReplicatedMessagesService replicated && !replicated.isPrimary()) {
			throw new WebApplicationException(Response.temporaryRedirect(targetUri).build());
		}
	}

	private URI buildPostMessageUri(String pwd) {
		return buildUri(RestMessages.PATH).query(RestMessages.PWD, pwd).build();
	}

	private URI buildGetMessageUri(String name, String mid, String pwd) {
		return buildUri(RestMessages.PATH)
				.path(RestMessages.MBOX)
				.path(name)
				.path(mid)
				.query(RestMessages.PWD, pwd)
				.build();
	}

	private URI buildGetMessagesUri(String name, String pwd, String query) {
		var builder = buildUri(RestMessages.PATH)
				.path(RestMessages.MBOX)
				.path(name)
				.query(RestMessages.PWD, pwd);
		if (query != null && !query.isEmpty()) {
			builder.query(RestMessages.QUERY, query);
		}
		return builder.build();
	}

	private URI buildRemoveInboxUri(String name, String mid, String pwd) {
		return buildUri(RestMessages.PATH)
				.path(RestMessages.MBOX)
				.path(name)
				.path(mid)
				.query(RestMessages.PWD, pwd)
				.build();
	}

	private URI buildDeleteMessageUri(String name, String mid, String pwd) {
		return buildUri(RestMessages.PATH)
				.path(name)
				.path(mid)
				.query(RestMessages.PWD, pwd)
				.build();
	}

	private RedirectUriBuilder buildUri(String path) {
		return new RedirectUriBuilder(((JavaReplicatedMessagesService) messages).primaryUri(), path);
	}

	private static final class RedirectUriBuilder {
		private final StringBuilder builder;

		private RedirectUriBuilder(URI baseUri, String path) {
			this.builder = new StringBuilder(baseUri.toString());
			if (builder.length() > 0 && builder.charAt(builder.length() - 1) == '/') {
				builder.setLength(builder.length() - 1);
			}
			builder.append(path);
		}

		private RedirectUriBuilder path(String segment) {
			if (builder.charAt(builder.length() - 1) != '/') {
				builder.append('/');
			}
			builder.append(segment);
			return this;
		}

		private RedirectUriBuilder query(String name, String value) {
			builder.append(builder.indexOf("?") < 0 ? '?' : '&');
			builder.append(name).append('=').append(value);
			return this;
		}

		private URI build() {
			return URI.create(builder.toString());
		}
	}
}
