package sd2526.trab.impl.rest.servers;

import java.util.logging.Logger;

import org.glassfish.jersey.server.ResourceConfig;

import sd2526.trab.api.java.Users;
import sd2526.trab.impl.utils.ServerSecret;

public class RestUsersServer extends AbstractRestServer {
	public static final int PORT = 3456;
	
	private static Logger Log = Logger.getLogger(RestUsersServer.class.getName());

	RestUsersServer(String[] args) {
		super( Log, Users.SERVICE_NAME , PORT);
		String secret = parseStringArg(args, "--server-secret", null);
		if (secret != null) ServerSecret.set(secret);
	}
	
	@Override
	void registerResources(ResourceConfig config) {
		config.register(RestUsersResource.class);
		config.register(ServerSecretFilter.class);
	}

	private static String parseStringArg(String[] args, String key, String defaultValue) {
		for (int i = 0; i < args.length - 1; i++) {
			if (key.equalsIgnoreCase(args[i])) return args[i + 1];
		}
		return defaultValue;
	}
	
	public static void main(String[] args) {
		new RestUsersServer(args).start();
	}	
}