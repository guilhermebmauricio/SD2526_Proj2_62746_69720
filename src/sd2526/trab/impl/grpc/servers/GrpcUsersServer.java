package sd2526.trab.impl.grpc.servers;

import java.io.IOException;
import java.util.List;
import java.util.logging.Logger;

import sd2526.trab.api.java.Users;
import sd2526.trab.impl.utils.ServerSecret;

public class GrpcUsersServer extends AbstractGrpcServer {
public static final int PORT = 13456;
	
	private static Logger Log = Logger.getLogger(GrpcUsersServer.class.getName());

	public GrpcUsersServer() {
		super( Log, Users.SERVICE_NAME, PORT);
	}
	
	@Override
	protected List<GrpcController> controllers(String uri) {
		return List.of( new GrpcUsersController(), new GrpcAdminUsersController() );
	}
	
	public static void main(String[] args) {
		try {
			for (int i = 0; i < args.length - 1; i++) {
				if ("--server-secret".equalsIgnoreCase(args[i])) {
					ServerSecret.set(args[i + 1]);
					break;
				}
			}
			new GrpcUsersServer().start();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	
}
