package sd2526.trab.impl.replication;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.List;
import java.util.logging.Logger;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

public class LeaderElection implements Watcher {
	private static Logger Log = Logger.getLogger(LeaderElection.class.getName());

	static {
		// summarizes the logging format
		System.setProperty("java.util.logging.SimpleFormatter.format", "%4$s: %5$s\n");
		System.setProperty("java.util.logging.level", "ERROR");
	}

	private String zookeeperAddress;
	private String myZNode;
	private String leader;
    private String leaderHost;
	private String myHost;
	private String zDir;
	private ZooKeeper zk;

    private final LeadershipListener listener;

	/**
	 * @param zooAddress  a string in the format host:port that allows to contact
	 *                    the zookeeper service
	 * @param serviceName the name of the service for which we are conducting leader
	 *                    election
	 */
	LeaderElection(String zooAddress, String serviceName, LeadershipListener listener) {
		this.zookeeperAddress = zooAddress;
		zDir = "/" + serviceName;
        this.listener = listener;

		try {
			myHost = InetAddress.getLocalHost().getHostName();
		} catch (UnknownHostException e) {
			myHost = "localhost";
		}

		leader = "leader";
        leaderHost = "";

		myZNode = "";
	}

	public void start() {
		try {
			zk = new ZooKeeper(zookeeperAddress, 3000, this);

			try {
				// Try to create the main znode for the service (might already exist)
				zk.create(zDir, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			} catch (KeeperException | InterruptedException e) {
				Log.info("Cloud not create the zkNode: " + zDir + "(" + e.getMessage() + ")");
			}

			// Create the ephemeral znode for the host itself
			myZNode = zk.create(zDir + "/host_", myHost.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,
					CreateMode.EPHEMERAL_SEQUENTIAL);

			Log.info("Created my personal znode: " + myZNode);
			
			myZNode = myZNode.substring(myZNode.lastIndexOf("/") + 1);

			//The watch will trigger immediatly because there is at least one node below the root zknode
			zk.getChildren(zDir, true); 
			
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void process(WatchedEvent event) {
		try {
			List<String> children = zk.getChildren(zDir, true);
			this.checkLeadership(children);
		} catch (KeeperException | InterruptedException e) {
			e.printStackTrace();
		}
	}

	private void checkLeadership(List<String> znodes) {
		try {
			Collections.sort(znodes);
			String currentLeader = znodes.get(0);

			Log.info("Leader znode: " + currentLeader);
			
			byte[] data = zk.getData(zDir + "/" + currentLeader, false, null);
			this.leaderHost = new String(data);
            boolean isLeader = myZNode.equals(currentLeader);
            boolean leaderChanged = !currentLeader.equals(this.leader);
            if(leaderChanged) {
                this.leader = currentLeader;
                listener.onLeadershipChange(isLeader, this.leader, this.leaderHost);
            }

			Log.info("Leader = '" + this.leader + "' running on host " + this.leaderHost + " ; my znode = " + myZNode
					+ " ; I am the leader = " + isLeader);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

    public boolean isLeader() {
        return myZNode.equals(this.leader);
    }

    public String getLeaderHost() {
        return this.leaderHost;
    }

}