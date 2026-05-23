package sd2526.trab.impl.utils;

public final class ServerSecret {

	private static volatile String value;

	public static void set(String secret) {
		if (value != null) throw new IllegalStateException("Server secret already set");
		value = secret;
	}

	public static String get() {
		return value;
	}

	public static boolean isValid(String candidate) {
		return value != null && value.equals(candidate);
	}
}
