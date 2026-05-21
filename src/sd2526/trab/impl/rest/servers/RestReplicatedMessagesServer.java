package sd2526.trab.impl.rest.servers;

import java.util.logging.Logger;

import org.glassfish.jersey.server.ResourceConfig;

import sd2526.trab.api.java.Messages;
import sd2526.trab.impl.java.servers.JavaReplicatedMessagesService;

public class RestReplicatedMessagesServer extends AbstractRestServer {

	public static final int PORT = 5567;
	private static final int DEFAULT_LOG_SIZE = 500;
	private static final int DEFAULT_PENDING_SIZE = 500;
	private static final String DEFAULT_ZOOKEEPER_ENDPOINT = "kafka:2181";

	private static final Logger Log = Logger.getLogger(RestReplicatedMessagesServer.class.getName());

	private final String[] args;

	RestReplicatedMessagesServer(String[] args) {
		super(Log, Messages.SERVICE_NAME, PORT);
		this.args = args;
	}

	@Override
	void registerResources(ResourceConfig config) {
		int logSize = JavaReplicatedMessagesService.parseIntArg(args, "--rep-log-size", DEFAULT_LOG_SIZE);
		int pendingSize = JavaReplicatedMessagesService.parseIntArg(args, "--rep-queue-size", DEFAULT_PENDING_SIZE);
		String zookeeperEndpoint = parseStringArg(args, "--zk", DEFAULT_ZOOKEEPER_ENDPOINT);

        // TODO: it's annoying me why this works here, but it had to be changed in the Gateway server to be class registering...
        // needs to be investigated
		var replicated = new JavaReplicatedMessagesService(super.serverURI, zookeeperEndpoint, logSize, pendingSize);
		replicated.startReplication();
		config.registerInstances(new RestReplicatedMessagesResource(replicated, replicated));
		config.registerInstances(new VersionHeaderHandler());
	}

	private static String parseStringArg(String[] args, String key, String defaultValue) {
		for (int i = 0; i < args.length - 1; i++) {
			if (key.equalsIgnoreCase(args[i])) {
				return args[i + 1];
			}
		}
		return defaultValue;
	}

	public static void main(String[] args) {
		new RestReplicatedMessagesServer(args).start();
	}
}
