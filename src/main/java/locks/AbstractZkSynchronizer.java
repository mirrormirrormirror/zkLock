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
public abstract class AbstractZkSynchronizer {
    private final ZooKeeper zk;
    private final String resourceName;
    private boolean hasLock = false;
    private int reenTranLockCount = 0;
    protected String ownerLockName = null;
    private final static String LOCK_PREFIX = "/lock_";
    protected static String READ_LOCK_PREFIX = "r_";
    protected static String WRITE_LOCK_PREFIX = "w_";

    AbstractZkSynchronizer(String zkHost, int sessionTimeout, String resourceName) throws IOException, InterruptedException {
        this.resourceName = resourceName;
        final CountDownLatch connectedSignal = new CountDownLatch(1);
        zk = new ZooKeeper(zkHost, sessionTimeout, new Watcher() {
            @Override
            public void process(WatchedEvent we) {
                if (we.getState() == Event.KeeperState.SyncConnected) {
                    connectedSignal.countDown();
                }
            }
        });
        connectedSignal.await();
    }

    protected int nonFairReaderPreviousWatchNodeIndex(List<String> locks) {
        for (int lockIndex = 0; lockIndex < locks.size(); lockIndex++) {
            if (locks.get(lockIndex).contains(READ_LOCK_PREFIX)) {
                return lockIndex - 1;
            }
        }
        return -1;
    }

    protected int fairReaderPreviousWatchNodeIndex(List<String> locks) {
        int ownerLockIndex = -1;
        for (int lockIndex = locks.size() - 1; lockIndex >= 0; lockIndex--) {
            if (locks.get(lockIndex).equals(ownerLockName)) {
                ownerLockIndex = lockIndex;
            }
        }

        for (int lockIndex = ownerLockIndex; lockIndex >= 0; lockIndex--) {
            if (locks.get(lockIndex).contains(READ_LOCK_PREFIX)) {
                return lockIndex;
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


    protected void createNodeResource() {
        try {
            zk.create(lockPath(), "0".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } catch (KeeperException ignored) {

        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    protected boolean betweenHead2ownerLockHasWriterLock(List<String> locks) {
        int ownerLockIndex = -1;
        for (int lockIndex = locks.size() - 1; lockIndex >= 0; lockIndex--) {
            if (locks.get(lockIndex).equals(ownerLockName)) {
                ownerLockIndex = lockIndex;
            }
        }

        for (int lockIndex = ownerLockIndex; lockIndex > 0; lockIndex--) {
            if (locks.get(lockIndex).contains(WRITE_LOCK_PREFIX)) {
                return true;
            }
        }
        return false;
    }

    protected int getReadLockCount() throws KeeperException, InterruptedException {
        byte[] lockPathContent = zk.getData(lockPath(), false, null);
        return Integer.valueOf(new String(lockPathContent));
    }

    protected void startupAddReadLockCount() {
        while (true) {
            try {
                Stat lockPathStat = zk.exists(lockPath(), false);
                int readLockCount = getReadLockCount();
                Stat updateLockPathResult = zk.setData(lockPath(), String.valueOf(readLockCount + 1).getBytes(), lockPathStat.getVersion());
                if (updateLockPathResult.getVersion() - lockPathStat.getVersion() == 1) {
                    break;
                }
            } catch (Exception e) {

            }
        }
    }


    protected boolean waitProcessAddReadLockCount() throws KeeperException, InterruptedException {
        while (true) {
            Stat lockPathStat = zk.exists(lockPath(), false);
            int readLockCount = getReadLockCount();
            if (readLockCount <= 0) {
                return false;
            }
            try {
                Stat updateLockPathResult = zk.setData(lockPath(), String.valueOf(readLockCount + 1).getBytes(), lockPathStat.getVersion());
                if (updateLockPathResult.getVersion() - lockPathStat.getVersion() == 1) {
                    return true;
                }
            } catch (Exception e) {

            }
        }
    }

    protected void minuReadLockCount() {
        while (true) {
            try {
                Stat lockPathStat = zk.exists(lockPath(), false);
                int readLockCount = getReadLockCount();
                System.out.println(readLockCount);
                Stat stat = zk.setData(lockPath(), String.valueOf(readLockCount - 1).getBytes(), lockPathStat.getVersion());
                if (stat.getVersion() - lockPathStat.getVersion() == 1) {
                    break;
                }
            } catch (Exception ignored) {

            }
        }
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


    protected void watchPreviousNode(String previousNodeName, long time, TimeUnit unit) {
        try {
            final CountDownLatch nodeDeleteSignal = new CountDownLatch(1);
            zk.getData(nodePath(previousNodeName), new Watcher() {
                @Override
                public void process(WatchedEvent event) {
                    if (event.getType() == Event.EventType.NodeDeleted) {
                        nodeDeleteSignal.countDown();
                    }
                }
            }, null);
            nodeDeleteSignal.await(time, unit);
        } catch (Exception e) {
            e.printStackTrace();
        }


    }


    protected void watchReadCount(long time, TimeUnit unit) throws KeeperException, InterruptedException {
        final CountDownLatch readCountDataChangeSignal = new CountDownLatch(1);
        byte[] readCountData = zk.getData(lockPath(), new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                if (event.getType() == Event.EventType.NodeDataChanged) {
                    readCountDataChangeSignal.countDown();
                }
            }
        }, null);
        Integer readCount = Integer.valueOf(new String(readCountData));
        if (readCount > 0) {
            readCountDataChangeSignal.await(time, unit);
        }
    }


    protected void watchReadCount() throws KeeperException, InterruptedException {
        final CountDownLatch readCountDataChangeSignal = new CountDownLatch(1);
        byte[] readCountData = zk.getData(lockPath(), new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                if (event.getType() == Event.EventType.NodeDataChanged) {
                    readCountDataChangeSignal.countDown();
                }
            }
        }, null);
        Integer readCount = Integer.valueOf(new String(readCountData));
        if (readCount > 0) {
            readCountDataChangeSignal.wait();
        }
    }

    protected void watchPreviousNode(String previousNodeName) {
        try {
            final CountDownLatch nodeDeleteSignal = new CountDownLatch(1);
            if (previousNodeName.contains(READ_LOCK_PREFIX) && ownerLockName.contains(READ_LOCK_PREFIX)) {
                return;
            }
            zk.getData(nodePath(previousNodeName), new Watcher() {
                @Override
                public void process(WatchedEvent event) {
                    if (event.getType() == Event.EventType.NodeDeleted) {
                        nodeDeleteSignal.countDown();
                    }
                }
            }, null);
            nodeDeleteSignal.await();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    protected boolean isOwnerLock() {
        return hasLock;
    }

    protected void setOwnerLock(boolean isOwnerLock) {
        hasLock = isOwnerLock;
        if (isOwnerLock) {
            reenTranLockCount = reenTranLockCount + 1;
        } else {
            reenTranLockCount = reenTranLockCount - 1;
        }
    }

}
