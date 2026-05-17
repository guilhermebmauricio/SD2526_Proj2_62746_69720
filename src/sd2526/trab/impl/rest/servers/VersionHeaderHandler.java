package sd2526.trab.impl.rest.servers;

import java.io.IOException;

import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerRequestFilter;
import jakarta.ws.rs.container.ContainerResponseContext;
import jakarta.ws.rs.container.ContainerResponseFilter;
import jakarta.ws.rs.ext.Provider;
import sd2526.trab.api.rest.RestMessages;

@Provider
public class VersionHeaderHandler implements ContainerRequestFilter, ContainerResponseFilter {

	private static final ThreadLocal<Long> requestVersion = new ThreadLocal<>();
	private static final ThreadLocal<Long> responseVersion = new ThreadLocal<>();

	@Override
	public void filter(ContainerRequestContext requestContext) throws IOException {
		responseVersion.remove();

		String value = requestContext.getHeaderString(RestMessages.HEADER_VERSION);
		if (value == null || value.isBlank()) {
			requestVersion.remove();
			return;
		}

		try {
			requestVersion.set(Long.valueOf(value));
		} catch (NumberFormatException ignored) {
			requestVersion.remove();
		}
	}

	@Override
	public void filter(ContainerRequestContext requestContext, ContainerResponseContext responseContext) throws IOException {
		var version = responseVersion.get();
		if (version != null) {
			responseContext.getHeaders().putSingle(RestMessages.HEADER_VERSION, Long.toString(version));
		}

		requestVersion.remove();
		responseVersion.remove();
	}

	public static Long requestVersion() {
		return requestVersion.get();
	}

	public static void responseVersion(long version) {
		responseVersion.set(version);
	}

	public static void clear() {
		requestVersion.remove();
		responseVersion.remove();
	}
}