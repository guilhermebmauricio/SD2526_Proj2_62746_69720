package sd2526.trab.impl.rest.servers;

import java.io.IOException;

import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerRequestFilter;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.Provider;
import sd2526.trab.impl.utils.ServerSecret;

@Provider
public class ServerSecretFilter implements ContainerRequestFilter {

	public static final String HEADER = "X-SERVER-SECRET";

	@Override
	public void filter(ContainerRequestContext ctx) throws IOException {
		if (ServerSecret.get() == null) return;

		String path = ctx.getUriInfo().getPath();
		if (!path.contains("/admin") && !path.contains("/replication")) return;

		if (!ServerSecret.isValid(ctx.getHeaderString(HEADER))) {
			ctx.abortWith(Response.status(Response.Status.FORBIDDEN).build());
		}
	}
}
