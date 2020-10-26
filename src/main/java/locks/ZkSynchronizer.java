package locks;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * @author mirror
 */
public class ZkSynchronizer {
    private final ZooKeeper zk;
    private final String resourceName;
    private boolean hasLock = false;
    private int reenTranLockCount = 0;
    protected String ownerLockName = null;
    private final static String LOCK_PREFIX = "/lock_";
    protected static String READ_LOCK_PREFIX = "r_";
    protected static String WRITE_LOCK_PREFIX = "w_";

    ZkSynchronizer(String zkHost, int sessionTimeout, String resourceName) throws IOException, InterruptedException {
        this.resourceName = resourceName;
        final CountDownLatch connectedSignal = new CountDownLatch(1);
        zk = new ZooKeeper(zkHost,sessionTimeout,new Watcher() {
            @Override
            public void process(WatchedEvent we) {
                if (we.getState() == Event.KeeperState.SyncConnected) {
                    connectedSignal.countDown();
                }
            }
        });
        connectedSignal.await();
    }

    protected int readerPreviousWatchNodeIndex(List<String> locks) {
        for (int lockIndex = 0; lockIndex < locks.size(); lockIndex++) {
            if (locks.get(lockIndex).contains(READ_LOCK_PREFIX)) {
                return lockIndex - 1;
            }
        }
        return -1;
    }


    protected int writerPreviousWatchNodeIndex(List<String> locks, String childrenName) {
        for (int lockIndex = locks.size() - 1; lockIndex >= 0; lockIndex--) {
            if (locks.get(lockIndex).equals(childrenName)) {
                return lockIndex - 1;
            }
        }
        return -1;
    }

    protected void addReenTranLock(int num) {
        reenTranLockCount = reenTranLockCount + num;
    }

    protected void minuReenTranLock(int num) {
        reenTranLockCount = reenTranLockCount - num;
    }

    protected int getReenTranLockCount() {
        return reenTranLockCount;
    }


    private String lockPath() {
        return LOCK_PREFIX + resourceName;
    }

    private String nodePath(String nodeName) {
        return lockPath() + "/" + nodeName;
    }


    protected boolean existsLockPath() {
        Stat exists = null;
        try {
            exists = zk.exists(lockPath(), false);
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return exists != null;
    }


    protected void createNodeResource() throws KeeperException, InterruptedException {
        if (existsLockPath()) {
            return;
        }
        zk.create(lockPath(), resourceName.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }


    protected String addChildren(String childrenName) throws KeeperException, InterruptedException {
        String nodePath = zk.create(nodePath(childrenName), childrenName.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        return nodePath.split("/")[2];
    }


    protected List<String> getChildrenList() throws KeeperException, InterruptedException {
        List<String> childrenList = zk.getChildren(lockPath(), false);
        Collections.sort(childrenList, new Comparator<String>() {
            @Override
            public int compare(String o1, String o2) {
                return Integer.valueOf(o1.split("_")[2]) > Integer.valueOf(o2.split("_")[2]) ? 1 : -1;
            }
        });

        return childrenList;
    }


    private void tryRemoveLockResource() {

        try {
            zk.delete(lockPath(), -1);
        } catch (Exception ignored) {

        }
    }


    protected void close() throws KeeperException, InterruptedException {
        reenTranLockCount = 0;
        zk.delete(nodePath(ownerLockName), -1);
        tryRemoveLockResource();
        zk.close();
    }

    protected void watchPreviousNode(String previousNodeName, long time, TimeUnit unit) throws KeeperException, InterruptedException {
        final CountDownLatch watchNodeSignal = new CountDownLatch(1);

        zk.getData(nodePath(previousNodeName), new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                if (event.getType() == Event.EventType.NodeDeleted) {
                    setOwnerLock(true);
                    watchNodeSignal.countDown();
                }
            }
        }, null);
        watchNodeSignal.await(time, unit);

    }


    protected void watchPreviousNode(String previousNodeName) {
        try {
            final CountDownLatch connectedSignal = new CountDownLatch(1);
            zk.getData(nodePath(previousNodeName), new Watcher() {
                @Override
                public void process(WatchedEvent event) {
                    if (event.getType() == Event.EventType.NodeDeleted) {
                        setOwnerLock(true);
                        connectedSignal.countDown();
                    }
                }
            }, null);
            connectedSignal.await();


        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    protected boolean isOwnerLock() {
        return hasLock;
    }

    protected void setOwnerLock(boolean isOwnerLock) {
        hasLock = isOwnerLock;
        reenTranLockCount = reenTranLockCount + 1;
    }


    protected void lockOwner(String nodeName) throws KeeperException, InterruptedException {
        setOwnerLock(true);
        Stat exists = zk.exists(nodePath(nodeName), false);
        if (exists == null) {
            setOwnerLock(false);
        }
    }
}
