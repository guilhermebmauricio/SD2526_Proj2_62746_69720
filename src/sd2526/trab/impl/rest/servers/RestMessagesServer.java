package sd2526.trab.impl.rest.servers;

import java.util.logging.Logger;

import org.glassfish.jersey.server.ResourceConfig;

import sd2526.trab.api.java.Messages;
import sd2526.trab.impl.utils.ServerSecret;

public class RestMessagesServer extends AbstractRestServer {
	public static final int PORT = 4567;
	
	private static Logger Log = Logger.getLogger(RestMessagesServer.class.getName());

	RestMessagesServer(String[] args) {
		super(Log, Messages.SERVICE_NAME, PORT);
		String secret = parseStringArg(args, "--server-secret", null);
		if (secret != null) ServerSecret.set(secret);
	}

	@Override
	void registerResources(ResourceConfig config) {
		config.register(RestMessagesResource.class);
		config.register(ServerSecretFilter.class);
	}

	private static String parseStringArg(String[] args, String key, String defaultValue) {
		for (int i = 0; i < args.length - 1; i++) {
			if (key.equalsIgnoreCase(args[i])) return args[i + 1];
		}
		return defaultValue;
	}

	public static void main(String[] args) {
		new RestMessagesServer(args).start();
	}
}