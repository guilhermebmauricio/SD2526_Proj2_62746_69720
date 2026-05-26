package sd2526.trab.impl.replication;

public interface LeadershipListener {
    void onLeadershipChange(boolean isLeader, String leaderZNode, String leaderHost);
}