package sd2526.trab.impl.oauth;

import java.util.List;

import com.github.scribejava.core.model.OAuth2AccessToken;
import com.github.scribejava.core.model.OAuthRequest;
import com.github.scribejava.core.model.Response;
import com.github.scribejava.core.model.Verb;
import com.github.scribejava.core.oauth.OAuth20Service;

import sd2526.trab.impl.oauth.zoho.ZohoServiceFactory;
import sd2526.trab.impl.oauth.zoho.ZohoTokenManager;
import sd2526.trab.impl.oauth.zoho.msgs.ZohoAccount;
import sd2526.trab.impl.oauth.zoho.msgs.ZohoAccountReply;
import sd2526.trab.impl.utils.JSON;

public class Zoho {
    public static final String MAIL_API_BASE = "https://mail.zoho.eu/api";
    static final String CLIENT_ID     = "1000.WMVLZ2SPETT552S9B33XM8JV0HDDCT";
    static final String CLIENT_SECRET = "77007f15bde61ec8a003b37ea86f368b9d7820a309";
    static final String REFRESH_TOKEN = "1000.02144fb20abb21c328c3383a7d24a6c4.89ecfdc2fbb38dbfb3137fc69f7bdd41";

    private static final String ACCOUNTS = "/accounts";

    final OAuth20Service service;
    final ZohoTokenManager tokenManager;

    static Zoho instance;

    private Zoho() {
        service = ZohoServiceFactory.buildService(CLIENT_ID, CLIENT_SECRET);
        tokenManager = new ZohoTokenManager(service, REFRESH_TOKEN);
    }

    synchronized public static Zoho getInstance() {
        if( instance == null ) instance = new Zoho();
        return instance;
    }

    public ZohoAccount getAccount() throws Exception {
        var url = MAIL_API_BASE + ACCOUNTS;
        System.err.println("getAccount URL: " + url);
        var request = signedRequest(Verb.GET, url);

        try (Response response = service.execute(request)) {
            System.err.println("getAccount response: " + response.getCode() + " / " + response.getBody());
            if (response.isSuccessful()) {
                var body = response.getBody();
                var data = JSON.decode(body, ZohoAccountReply.class).data();
                if (data == null || data.isEmpty()) return null;
                return data.get(0);
            } else {
                System.err.println(response.getCode() + "/" + response.getBody());
                return null;
            }
        }
    }

    public String getInboxFolderId(String accountId) throws Exception {
        var request = signedRequest(Verb.GET, MAIL_API_BASE + ACCOUNTS + "/" + accountId + "/folders");

        try (Response response = service.execute(request)) {
            if (!response.isSuccessful()) {
                System.err.println("getInboxFolderId: " + response.getCode() + "/" + response.getBody());
                return null;
            }
            var reply = JSON.decode(response.getBody(), ZohoFolderListReply.class);
            if (reply == null || reply.data() == null) return null;
            for (var folder : reply.data())
                if ("Inbox".equalsIgnoreCase(folder.folderName()))
                    return folder.folderId();
            return null;
        }
    }

    public String getSentFolderId(String accountId) throws Exception {
        var request = signedRequest(Verb.GET, MAIL_API_BASE + ACCOUNTS + "/" + accountId + "/folders");

        try (Response response = service.execute(request)) {
            if (!response.isSuccessful()) {
                System.err.println("getSentFolderId: " + response.getCode() + "/" + response.getBody());
                return null;
            }
            var reply = JSON.decode(response.getBody(), ZohoFolderListReply.class);
            if (reply == null || reply.data() == null) return null;
            for (var folder : reply.data())
                if ("Sent".equalsIgnoreCase(folder.folderName()))
                    return folder.folderId();
            return null;
        }
    }

    public boolean sendMessage(String accountId, String fromAddress, String subject, String body) throws Exception {
        var url     = MAIL_API_BASE + ACCOUNTS + "/" + accountId + "/messages";
        var request = signedRequest(Verb.POST, url);
        request.addHeader("Content-Type", "application/json");

        var payload = """
                {
                  "fromAddress": "%s",
                  "toAddress":   "%s",
                  "subject":     "%s",
                  "content":     %s
                }""".formatted(
                fromAddress,
                fromAddress,
                subject.replace("\"", "\\\""),
                JSON.encode(body));

        request.setPayload(payload);

        try (Response response = service.execute(request)) {
            System.err.println("sendMessage response: " + response.getCode() + "/" + response.getBody());
            if (!response.isSuccessful()) {
                System.err.println("sendMessage: " + response.getCode() + "/" + response.getBody());
                return false;
            }
            return true;
        }
    }

    public List<EmailSummary> listMessages(String accountId, String folderId) throws Exception {
        var url = MAIL_API_BASE + ACCOUNTS + "/" + accountId
                + "/messages/view?folderId=" + folderId + "&limit=200";
        var request = signedRequest(Verb.GET, url);

        try (Response response = service.execute(request)) {
            if (!response.isSuccessful()) {
                System.err.println("listMessages: " + response.getCode() + "/" + response.getBody());
                return List.of();
            }
            var reply = JSON.decode(response.getBody(), ZohoMessageListReply.class);
            return (reply != null && reply.data() != null) ? reply.data() : List.of();
        }
    }

    public String getMessageContent(String accountId, String folderId, String messageId) throws Exception {
        var url = MAIL_API_BASE + ACCOUNTS + "/" + accountId + "/folders/" + folderId + "/messages/" + messageId + "/content";
        var request = signedRequest(Verb.GET, url);

        try (Response response = service.execute(request)) {
            if (!response.isSuccessful()) {
                System.err.println("getMessageContent: " + response.getCode() + "/" + response.getBody());
                return null;
            }
            System.err.println("getMessageContent raw body: " + response.getBody());
            var reply = JSON.decode(response.getBody(), ZohoMessageContentReply.class);
            return (reply != null && reply.data() != null) ? reply.data().content() : null;
        }
    }

    public boolean deleteMessage(String accountId, String folderId, String messageId) throws Exception {
        var url = MAIL_API_BASE + "/accounts/" + accountId + "/folders/" + folderId + "/messages/" + messageId;
        var request = signedRequest(Verb.DELETE, url);

        try (Response response = service.execute(request)) {
            if (!response.isSuccessful()) {
                System.err.println("deleteMessage: " + response.getCode() + "/" + response.getBody());
                return false;
            }
            return true;
        }
    }

    private OAuthRequest signedRequest(Verb verb, String url) throws Exception {
        var token   = new OAuth2AccessToken(tokenManager.getValidAccessToken());
        var request = new OAuthRequest(verb, url);
        service.signRequest(token, request);
        return request;
    }

    public static class EmailSummary {
        String messageId;
        String folderId;
        String subject;

        public String messageId() { return messageId; }
        public String folderId()  { return folderId; }
        public String subject()   { return subject; }
    }

    record ZohoFolder(String folderId, String folderName) {}
    record ZohoFolderListReply(List<ZohoFolder> data) {}
    record ZohoMessageListReply(List<EmailSummary> data) {}
    record ZohoMessageContent(String content) {}
    record ZohoMessageContentReply(ZohoMessageContent data) {}
}