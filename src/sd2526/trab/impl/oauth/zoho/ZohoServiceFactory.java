package sd2526.trab.impl.oauth.zoho;

import com.github.scribejava.core.builder.ServiceBuilder;
import com.github.scribejava.core.httpclient.jdk.JDKHttpClientConfig;
import com.github.scribejava.core.oauth.OAuth20Service;

public class ZohoServiceFactory {

    private static final int CONNECT_TIMEOUT_MS = 5_000;
    private static final int READ_TIMEOUT_MS    = 10_000;

    public static OAuth20Service buildService(String clientId, String clientSecret) {
        var httpConfig = JDKHttpClientConfig.defaultConfig();
        httpConfig.setConnectTimeout(CONNECT_TIMEOUT_MS);
        httpConfig.setReadTimeout(READ_TIMEOUT_MS);

        return new ServiceBuilder(clientId)
                .apiSecret(clientSecret)
                .defaultScope("ZohoMail.messages.ALL,ZohoMail.accounts.READ,ZohoMail.folders.READ")
                .httpClientConfig(httpConfig)
                .build(ZohoApi20.instance());
    }
}