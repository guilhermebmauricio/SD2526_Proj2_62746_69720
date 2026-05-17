package sd2526.trab.impl.rest.servers;

import java.util.logging.Logger;

import org.glassfish.jersey.server.ResourceConfig;

import sd2526.trab.api.java.Messages;
import sd2526.trab.impl.java.servers.JavaReplicatedMessagesService;

public class RestReplicatedMessagesServer extends AbstractRestServer {

	public static final int PORT = 5567;
	private static final int DEFAULT_LOG_SIZE = 500;
	private static final int DEFAULT_PENDING_SIZE = 500;

	private static final Logger Log = Logger.getLogger(RestReplicatedMessagesServer.class.getName());

	private final String[] args;

	RestReplicatedMessagesServer(String[] args) {
		super(Log, Messages.SERVICE_NAME, PORT);
		this.args = args;
	}

	@Override
	void registerResources(ResourceConfig config) {
		var role = JavaReplicatedMessagesService.parseRole(args);
		int logSize = JavaReplicatedMessagesService.parseIntArg(args, "--rep-log-size", DEFAULT_LOG_SIZE);
		int pendingSize = JavaReplicatedMessagesService.parseIntArg(args, "--rep-queue-size", DEFAULT_PENDING_SIZE);

        // TODO: it's annoying me why this works here, but it had to be changed in the Gateway server to be class registering...
        // needs to be investigated
		var replicated = new JavaReplicatedMessagesService(role, super.serverURI, logSize, pendingSize);
		config.registerInstances(new RestReplicatedMessagesResource(replicated, replicated));
		config.registerInstances(new VersionHeaderHandler());
	}

	public static void main(String[] args) {
		new RestReplicatedMessagesServer(args).start();
	}
}
