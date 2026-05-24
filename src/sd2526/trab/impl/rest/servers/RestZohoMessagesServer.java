package sd2526.trab.impl.rest.servers;

import java.util.logging.Logger;

import org.glassfish.jersey.server.ResourceConfig;

import sd2526.trab.api.java.Messages;
import sd2526.trab.impl.java.servers.ZohoMessages;
import sd2526.trab.impl.utils.IP;

public class RestZohoMessagesServer extends AbstractRestServer {

    public static final int PORT = 4567;

    private static final Logger Log = Logger.getLogger(RestZohoMessagesServer.class.getName());

    RestZohoMessagesServer() { super(Log, Messages.SERVICE_NAME, PORT); }

    @Override
    void registerResources(ResourceConfig config) { config.register(RestZohoMessagesResource.class); }

    public static void main(String[] args) throws Exception {
        for (int i = 0; i < args.length; i++) System.err.println("args[" + i + "] = " + args[i]);

        boolean cleanState = args.length > 0 && Boolean.parseBoolean(args[0]);
        String  localUser  = args.length > 1 ? args[1] : "zoho";
        String  domain     = IP.domain();

        Log.info("Starting RestZohoMessagesServer – user=%s domain=%s cleanState=%b"
                .formatted(localUser, domain, cleanState));

        try {
            ZohoMessages.getInstance().init(localUser, domain, cleanState);
        } catch (Exception e) {
            System.err.println("FATAL: ZohoMessages init failed: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }

        new RestZohoMessagesServer().start();
    }
}