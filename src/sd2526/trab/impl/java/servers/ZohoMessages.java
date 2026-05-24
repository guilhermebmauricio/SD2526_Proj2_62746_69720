package sd2526.trab.impl.java.servers;

import static sd2526.trab.api.java.Result.error;
import static sd2526.trab.api.java.Result.ok;
import static sd2526.trab.api.java.Result.ErrorCode.*;

import java.util.List;
import java.util.logging.Logger;
import java.util.ArrayList;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import sd2526.trab.api.Message;
import sd2526.trab.api.java.Messages;
import sd2526.trab.api.java.Result;
import sd2526.trab.impl.oauth.Zoho;
import sd2526.trab.impl.api.java.AdminMessages;
import sd2526.trab.impl.java.clients.Clients;
import sd2526.trab.impl.utils.JSON;
import sd2526.trab.impl.utils.Sleep;

public class ZohoMessages implements Messages, AdminMessages {

    private static final Logger Log = Logger.getLogger(ZohoMessages.class.getName());

    private static final String SEPARATOR = "\n---MESSAGE_METADATA_SEPARATOR---\n";
    private static final long REMOTE_DEADLINE = 90000;

    private static ZohoMessages instance;

    private final Zoho zoho = Zoho.getInstance();
    private final JobDispatcher jobs = new JobDispatcher();
    private final Map<String, Message> cache = new ConcurrentHashMap<>();

    private volatile boolean cleaning = false;

    private String accountId;
    private String zohoEmailAddress;
    private String sentFolderId;
    private String localUser;
    private String domain;

    private ZohoMessages() {}

    public synchronized static ZohoMessages getInstance() {
        if (instance == null)
            instance = new ZohoMessages();
        return instance;
    }

    public void init(String localUser, String domain, boolean cleanState) throws Exception {
        this.localUser = localUser;
        this.domain    = domain;

        var account = zoho.getAccount();
        if (account == null) throw new RuntimeException("Failed to fetch Zoho account — check credentials");

        this.accountId        = account.accountId();
        this.zohoEmailAddress = account.primaryEmailAddress();
        this.sentFolderId     = zoho.getSentFolderId(accountId);

        if (sentFolderId == null) throw new RuntimeException("Failed to find Zoho sent folder");

        Log.info("ZohoMessages init – accountId=%s address=%s user=%s@%s cleanState=%b sentFolderId=%s"
                .formatted(accountId, zohoEmailAddress, localUser, domain, cleanState, sentFolderId));

        if (cleanState) {
            cache.clear();
            cleaning = true;
            new Thread(() -> {
                try { cleanInbox(); }
                catch (Exception e) { Log.warning("cleanInbox failed: " + e.getMessage()); }
                finally { cleaning = false; }
            }).start();
        }
    }

    @Override
    public Result<String> postMessage(String pwd, Message msg) {
        Log.info("postMessage sender=%s".formatted(msg.getSender()));
        if (badParams(msg.getSender(), pwd) || msg.getDestination() == null || msg.getDestination().isEmpty())
            return error(BAD_REQUEST);

        var senderName = msg.getSender().contains("@") ? msg.getSender().split("@")[0] : msg.getSender();
        var userResult = Clients.UsersClient.get().getUser(senderName, pwd);
        if (!userResult.isOK()) return error(userResult.error());

        var sender = userResult.value();
        String id  = "%s+%s".formatted(domain, UUID.randomUUID());
        msg.setId(id);
        msg.setSender("%s <%s@%s>".formatted(sender.getDisplayName(), sender.getName(), sender.getDomain()));

        for (String dest : msg.getDestination()) {
            var parts      = dest.split("@", 2);
            var destDomain = parts.length == 2 ? parts[1] : domain;

            if (destDomain.equals(domain)) {
                var r = storeMessage(msg);
                if (!r.isOK()) return error(r.error());
            } else {
                final var rd   = destDomain;
                final var addr = dest;
                jobs.submit(rd, () -> {
                    var r = reTry(() -> Clients.AdminMessagesClient.get(rd).remotePostMessage(msg), REMOTE_DEADLINE);
                    if (r.error() == Result.ErrorCode.TIMEOUT) storeMessage(msg.cloneWithTimeout(addr));
                });
            }
        }
        return ok(id);
    }

    @Override
    public Result<Message> getInboxMessage(String name, String mid, String pwd) {
        Log.info("getInboxMessage user=%s mid=%s".formatted(name, mid));
        if (badParams(name, mid, pwd)) return error(BAD_REQUEST);

        var auth = Clients.UsersClient.get().getUser(name, pwd);
        if (!auth.isOK()) return error(auth.error());

        var cached = cache.get(mid);
        if (cached != null) return ok(cached);

        try {
            for (var summary : zoho.listMessages(accountId, sentFolderId)) {
                var msg = fetchAndParse(summary);
                if (msg != null && mid.equals(msg.getId())) return ok(msg);
            }
            return error(NOT_FOUND);
        } catch (Exception e) {
            Log.warning("getInboxMessage: " + e.getMessage());
            return error(INTERNAL_ERROR);
        }
    }

    @Override
    public Result<List<String>> getAllInboxMessages(String name, String pwd) {
        Log.info("getAllInboxMessages user=%s".formatted(name));
        if (badParams(name, pwd)) return error(BAD_REQUEST);

        var auth = Clients.UsersClient.get().getUser(name, pwd);
        if (!auth.isOK()) return error(auth.error());

        try {
            var summaries = zoho.listMessages(accountId, sentFolderId);
            Log.info("Zoho listMessages returned %d summaries".formatted(summaries.size()));
            for (var s : summaries)
                Log.info("  summary: messageId=%s folderId=%s subject=%s".formatted(s.messageId(),
                        s.folderId(), s.subject()));
        } catch (Exception e) { Log.warning("debug listMessages: " + e.getMessage()); }

        try {
            var ids = new ArrayList<String>(cache.keySet());
            for (var summary : zoho.listMessages(accountId, sentFolderId)) {
                var msg = fetchAndParse(summary);
                if (msg != null && !ids.contains(msg.getId())) {
                    ids.add(msg.getId());
                    cache.put(msg.getId(), msg);
                }
            }
            return ok(ids);
        } catch (Exception e) {
            Log.warning("getAllInboxMessages: " + e.getMessage());
            return ok( new ArrayList<>(cache.keySet()) );
        }
    }

    @Override
    public Result<Void> removeInboxMessage(String name, String mid, String pwd) {
        Log.info("removeInboxMessage user=%s mid=%s".formatted(name, mid));
        if (badParams(name, mid, pwd)) return error(BAD_REQUEST);

        var auth = Clients.UsersClient.get().getUser(name, pwd);
        if (!auth.isOK()) return error(auth.error());

        return deleteByMid(mid);
    }

    @Override
    public Result<Void> deleteMessage(String name, String mid, String pwd) {
        Log.info("deleteMessage name=%s mid=%s".formatted(name, mid));
        if (badParams(name, mid, pwd)) return error(BAD_REQUEST);

        var auth = Clients.UsersClient.get().getUser(name, pwd);
        if (!auth.isOK()) return error(auth.error());

        try {
            for (var summary : zoho.listMessages(accountId, sentFolderId)) {
                var msg = fetchAndParse(summary);
                if (msg != null && mid.equals(msg.getId())) {
                    if (!name.equals(msg.senderName())) return error(FORBIDDEN);

                    zoho.deleteMessage(accountId, summary.folderId(), summary.messageId());
                    cache.remove(mid);

                    for (String dest : msg.getDestination()) {
                        var parts   = dest.split("@", 2);
                        var dDomain = parts.length == 2 ? parts[1] : domain;
                        if (!dDomain.equals(domain)) {
                            final var rd = dDomain;
                            jobs.submit(rd, () -> reTry(() ->
                                    Clients.AdminMessagesClient.get(rd).remoteDeleteMessage(mid), REMOTE_DEADLINE));
                        }
                    }
                    return ok();
                }
            }
            return ok();
        } catch (Exception e) {
            Log.warning("deleteMessage: " + e.getMessage());
            return error(INTERNAL_ERROR);
        }
    }

    @Override
    public Result<List<String>> searchInbox(String name, String pwd, String query) {
        Log.info("searchInbox user=%s query=%s".formatted(name, query));
        if (badParams(name, pwd)) return error(BAD_REQUEST);

        var auth = Clients.UsersClient.get().getUser(name, pwd);
        if (!auth.isOK()) return error(auth.error());

        var q = query.toLowerCase();
        try {
            var ids = new ArrayList<String>();
            for (var msg : cache.values())
                if (matches(msg, q)) ids.add(msg.getId());
            for (var summary : zoho.listMessages(accountId, sentFolderId)) {
                var msg = fetchAndParse(summary);
                if (msg != null && !cache.containsKey(msg.getId())) {
                    cache.put(msg.getId(), msg);
                    if (matches(msg, q)) ids.add(msg.getId());
                }
            }
            return ok(ids);
        } catch (Exception e) {
            Log.warning("searchInbox: " + e.getMessage());
            return error(INTERNAL_ERROR);
        }
    }

    @Override
    public Result<Void> remotePostMessage(Message m) {
        Log.info("remotePostMessage id=%s".formatted(m.getId()));
        return storeMessage(m);
    }

    @Override
    public Result<Void> remoteDeleteMessage(String mid) {
        Log.info("remoteDeleteMessage mid=%s".formatted(mid));
        return deleteByMid(mid);
    }

    @Override
    public Result<Void> remoteDeleteUserInbox(String name) {
        Log.info("remoteDeleteUserInbox user=%s".formatted(name));
        if (!localUser.equals(name)) return error(NOT_FOUND);
        try {
            cleanInbox();
            cache.clear();
            return ok();
        } catch (Exception e) {
            return error(INTERNAL_ERROR);
        }
    }

    private Message fetchAndParse(Zoho.EmailSummary summary) {
        try {
            var body = zoho.getMessageContent(accountId, summary.folderId(), summary.messageId());
            return parseBody(body);
        } catch (Exception e) {
            Log.warning("fetchAndParse: " + e.getMessage());
            return null;
        }
    }

    private Message parseBody(String body) {
        if (body == null) return null;

        // remove HTML tags
        body = body.replaceAll("<[^>]*>", "").trim();

        // replace HTML entities
        body = body.replace("&quot;", "\"")
                .replace("&#39;", "'")
                .replace("&amp;", "&")
                .replace("&lt;", "<")
                .replace("&gt;", ">")
                .replace("&nbsp;", " ");

        int sepIdx = body.lastIndexOf(SEPARATOR);
        if (sepIdx < 0) {
            var meta = JSON.decode(body, StoredMetadata.class);
            if (meta == null) return null;
            return metadataToMsg(meta);
        }

        var contents = body.substring(0, sepIdx);
        var metaJson = body.substring(sepIdx + SEPARATOR.length());
        var meta     = JSON.decode(metaJson, StoredMetadata.class);
        if (meta == null) return null;
        var msg = metadataToMsg(meta);
        if (msg != null && (msg.getContents() == null || msg.getContents().isEmpty())) msg.setContents(contents);
        return msg;
    }

    private Message metadataToMsg(StoredMetadata meta) {
        var msg = new Message();
        msg.setId(meta.id());
        msg.setSender(meta.sender());
        msg.setDestination(Set.copyOf(meta.destination()));
        msg.setSubject(meta.subject());
        msg.setCreationTime(meta.creationTime());
        msg.setContents(meta.contents());
        return msg;
    }

    // deletes all messages in zoho sent folder
    private void cleanInbox() throws Exception {
        Log.info("Cleaning Zoho inbox...");
        for (var summary : zoho.listMessages(accountId, sentFolderId))
            zoho.deleteMessage(accountId, summary.folderId(), summary.messageId());
        Log.info("Inbox cleaned.");
    }

    private void deleteEmail(String folderId, String zohoMsgId) throws Exception {
        zoho.deleteMessage(accountId, folderId, zohoMsgId);
    }

    // lists email summaries in zoho sent folder
    private List<Zoho.EmailSummary> listEmailSummaries() throws Exception {
        return zoho.listMessages(accountId, sentFolderId);
    }

    private Result<Void> deleteByMid(String mid) {
        // checks cache before zoho
        boolean wasInCache = cache.remove(mid) != null;
        try {
            for (var summary : zoho.listMessages(accountId, sentFolderId)) {
                var msg = fetchAndParse(summary);
                if (msg != null && mid.equals(msg.getId())) {
                    zoho.deleteMessage(accountId, summary.folderId(), summary.messageId());
                    return ok();
                }
            }
            return wasInCache ? ok() : error(NOT_FOUND);
        } catch (Exception e) {
            Log.warning("deleteByMid: " + e.getMessage());
            return error(INTERNAL_ERROR);
        }
    }

    private Result<Void> storeMessage(Message msg) {
        while (cleaning) Sleep.ms(100);

        try {
            var meta = new StoredMetadata(
                    msg.getId(),
                    msg.getSender(),
                    new ArrayList<>(msg.getDestination()),
                    msg.getSubject(),
                    msg.getContents(),
                    msg.getCreationTime());

            var body    = JSON.encode(meta);
            var subject = msg.getSubject() != null ? msg.getSubject() : "(no subject)";

            Log.info("storeMessage id=%s sentFolderId=%s".formatted(msg.getId(), sentFolderId));
            boolean ok = zoho.sendMessage(accountId, zohoEmailAddress, subject, body);
            Log.info("storeMessage result=%b".formatted(ok));
            if (ok) {
                cache.put(msg.getId(), msg);
                return ok();
            }
            return error(INTERNAL_ERROR);
        } catch (Exception e) {
            Log.warning("storeMessage: " + e.getMessage());
            return error(INTERNAL_ERROR);
        }
    }

    private boolean matches(Message msg, String s) {
        return (msg.getSubject()  != null && msg.getSubject().toLowerCase().contains(s))
                || (msg.getContents() != null && msg.getContents().toLowerCase().contains(s));
    }

    private boolean badParams(String... params) {
        for (var p : params) if (p == null || p.isBlank()) return true;
        return false;
    }

    private <T> Result<T> reTry(java.util.function.Supplier<Result<T>> action, long deadlineMs) {
        var start = System.currentTimeMillis();
        Result<T> last;
        do {
            last = action.get();
            if (last.error() != Result.ErrorCode.TIMEOUT) return last;
        } while (System.currentTimeMillis() - start < deadlineMs);
        return last;
    }

    record StoredMetadata(
            String       id,
            String       sender,
            List<String> destination,
            String       subject,
            String       contents,
            long         creationTime) {}

    private static class JobDispatcher {
        private final ConcurrentHashMap<String, ExecutorService> executors = new ConcurrentHashMap<>();

        public void submit(String domain, Runnable job) {
            ExecutorService executor = executors.computeIfAbsent(
                    domain,
                    d -> Executors.newSingleThreadExecutor(r -> {
                        Thread t = new Thread(r);
                        t.setUncaughtExceptionHandler((thr, ex) -> ex.printStackTrace());
                        return t;
                    })
            );
            executor.submit(job);
        }
    }
}