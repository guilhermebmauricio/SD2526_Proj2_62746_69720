package sd2526.trab.impl.rest.servers;

import java.util.List;
import java.net.URI;

import jakarta.inject.Singleton;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.Response;
import sd2526.trab.api.Message;
import sd2526.trab.api.java.Messages;
import sd2526.trab.api.rest.RestMessages;
import sd2526.trab.impl.api.java.AdminMessages;
import sd2526.trab.impl.api.java.ReplicationMessages;
import sd2526.trab.impl.api.rest.RestAdminMessages;
import sd2526.trab.impl.api.rest.RestReplicationMessages;
import sd2526.trab.impl.java.servers.JavaReplicatedMessagesService;
import sd2526.trab.impl.replication.ReplicationAck;
import sd2526.trab.impl.replication.ReplicationCatchupResponse;
import sd2526.trab.impl.replication.ReplicationOperation;

@Singleton
public class RestReplicatedMessagesResource extends RestResource implements RestMessages, RestAdminMessages, RestReplicationMessages {

	private final Messages messages;
	private final AdminMessages admin;
	private final ReplicationMessages replication;

	public RestReplicatedMessagesResource(Messages messages, AdminMessages admin, ReplicationMessages replication) {
		this.messages = messages;
		this.admin = admin;
		this.replication = replication;
	}

	@Override
	public String postMessage(String pwd, Message msg) {
		redirectOrRejectWriteIfNotWritable(buildPostMessageUri(pwd));
		try {
			return super.resultOrThrow(messages.postMessage(pwd, msg));
		} finally {
			publishResponseVersion();
		}
	}

	@Override
	public Message getMessage(String name, String mid, String pwd) {
		redirectStaleReadIfNeeded(buildGetMessageUri(name, mid, pwd));
		try {
			return super.resultOrThrow(messages.getInboxMessage(name, mid, pwd));
		} finally {
			publishResponseVersion();
		}
	}

	@Override
	public List<String> getMessages(String name, String pwd, String query) {
		redirectStaleReadIfNeeded(buildGetMessagesUri(name, pwd, query));
		try {
			if (query != null && !query.isEmpty()) {
				return super.resultOrThrow(messages.searchInbox(name, pwd, query));
			}
			return super.resultOrThrow(messages.getAllInboxMessages(name, pwd));
		} finally {
			publishResponseVersion();
		}
	}

	@Override
	public void removeFromUserInbox(String name, String mid, String pwd) {
		redirectOrRejectWriteIfNotWritable(buildRemoveInboxUri(name, mid, pwd));
		try {
			super.resultOrThrow(messages.removeInboxMessage(name, mid, pwd));
		} finally {
			publishResponseVersion();
		}
	}

	@Override
	public void deleteMessage(String name, String mid, String pwd) {
		redirectOrRejectWriteIfNotWritable(buildDeleteMessageUri(name, mid, pwd));
		try {
			super.resultOrThrow(messages.deleteMessage(name, mid, pwd));
		} finally {
			publishResponseVersion();
		}
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
	public Long getCurrentVersion() {
		return super.resultOrThrow(replication.getCurrentVersion());
	}

	@Override
	public ReplicationAck replicateOperation(ReplicationOperation op) {
		return super.resultOrThrow(replication.replicateOperation(op));
	}

	@Override
	public ReplicationCatchupResponse getOperationsAfter(long seq, int limit) {
		return super.resultOrThrow(replication.getOperationsAfter(seq, limit));
	}

	private void redirectOrRejectWriteIfNotWritable(URI targetUri) {
		var replicated = replicatedMessages();
		if (replicated.isWritablePrimary()) {
			return;
		}

		publishResponseVersion();
		if (!replicated.isPrimary()) {
			throw new WebApplicationException(Response.temporaryRedirect(targetUri).build());
		}

		throw new WebApplicationException(Response.status(Response.Status.SERVICE_UNAVAILABLE).build());
	}

	private void redirectStaleReadIfNeeded(URI targetUri) {
		var replicated = replicatedMessages();
		Long expectedVersion = VersionHeaderHandler.requestVersion();
		if (!replicated.shouldRedirectRead(expectedVersion)) {
			return;
		}

		replicated.triggerCatchUpToVersion(expectedVersion);
		publishResponseVersion();
		throw new WebApplicationException(Response.temporaryRedirect(targetUri).build());
	}

	private void publishResponseVersion() {
		long currentVersion = replicatedMessages().currentVersion();
		Long expectedVersion = VersionHeaderHandler.requestVersion();
		if (expectedVersion != null) {
			currentVersion = Math.max(currentVersion, expectedVersion);
		}
		VersionHeaderHandler.responseVersion(currentVersion);
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

	private JavaReplicatedMessagesService replicatedMessages() {
		return (JavaReplicatedMessagesService) messages;
	}

	private RedirectUriBuilder buildUri(String path) {
		return new RedirectUriBuilder(replicatedMessages().primaryUri(), path);
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
			while (!segment.isEmpty() && segment.charAt(0) == '/') {
				segment = segment.substring(1);
			}
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
