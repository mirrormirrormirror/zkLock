package locks;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * 此类封装了一些对zookeeper的操作，为zkLock提供了锁元语与zookeeper的映射
 *
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


    /**
     * 读锁需要监控本客户端前面最近的一个写节点，此方法是返回这个写锁的位置
     *
     * @param locks lock_path下的子节点列表
     * @return
     */
    protected int readerPreviousWatchNodeIndex(List<String> locks) {
        int ownerLockIndex = -1;
        for (int lockIndex = locks.size() - 1; lockIndex >= 0; lockIndex--) {
            if (locks.get(lockIndex).equals(ownerLockName)) {
                ownerLockIndex = lockIndex;
            }
        }

        for (int lockIndex = ownerLockIndex; lockIndex >= 0; lockIndex--) {
            if (locks.get(lockIndex).contains(WRITE_LOCK_PREFIX)) {
                return lockIndex;
            }
        }
        return -1;
    }


    /**
     * 写锁需要监控本客户端前面最近的一个写节点，此方法是返回这个节点的位置
     *
     * @param locks
     * @return
     */
    protected int writerPreviousWatchNodeIndex(List<String> locks) {
        for (int lockIndex = locks.size() - 1; lockIndex >= 0; lockIndex--) {
            if (locks.get(lockIndex).equals(ownerLockName)) {
                return lockIndex - 1;
            }
        }
        return -1;
    }

    /**
     * 重入锁加锁
     *
     * @param num 加锁的数量
     */
    protected void addReenTranLock(int num) {
        reenTranLockCount = reenTranLockCount + num;
    }

    /**
     * 重入锁释放
     *
     * @param num 释放锁的数量
     */
    protected void minuReenTranLock(int num) {
        reenTranLockCount = reenTranLockCount - num;
    }

    /**
     * 获取现在重入锁的个数
     *
     * @return
     */
    protected int getReenTranLockCount() {
        return reenTranLockCount;
    }


    /**
     * 锁资源名称
     *
     * @return
     */
    private String lockPath() {
        return LOCK_PREFIX + resourceName;
    }

    /**
     * 根据子节点名称返回zookeeper全路径
     *
     * @param nodeName
     * @return
     */
    private String nodePath(String nodeName) {
        return lockPath() + "/" + nodeName;
    }


    /**
     * 判断锁资源节点是否存在
     *
     * @return
     */
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


    /**
     * 创建锁资源节点
     */
    protected void createNodeResource() {
        try {
            zk.create(lockPath(), "0".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } catch (KeeperException ignored) {

        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * 判断本客户端和头节点之间是否存在写节点
     *
     * @param locks
     * @return
     */
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

    /**
     * 在锁资源节点中获取节点的内容--读锁占用个数
     *
     * @return
     * @throws KeeperException
     * @throws InterruptedException
     */
    protected int getReadLockCount() throws KeeperException, InterruptedException {
        byte[] lockPathContent = zk.getData(lockPath(), false, null);
        return Integer.valueOf(new String(lockPathContent));
    }

    /**
     * 如果读锁在队头则需要进行原子性加1操作，而这个操作逻辑上是一定成功的，如果有异常就是zookeeper的异常
     * @throws KeeperException
     * @throws InterruptedException
     */
    protected void startupAddReadLockCount() throws KeeperException, InterruptedException {
        Stat lockPathStat = zk.exists(lockPath(), false);
        int readLockCount = getReadLockCount();
        zk.setData(lockPath(), String.valueOf(readLockCount + 1).getBytes(), lockPathStat.getVersion());
    }


    /**
     * 如果读锁不在队头，但是此时read_count大于0，则可以尝试进行原子性加1，但是这个加1也有可能失败，因为在加1前可能有其他客户端已经对目标节点操作过
     * @return
     * @throws KeeperException
     * @throws InterruptedException
     */
    protected boolean attemptAddReadLockCount() throws KeeperException, InterruptedException {
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
            } catch (Exception ignored) {

            }
        }
    }

    /**
     * 释放读锁的时候需要对锁资源节点的read_count进行原子性的减1
     */
    protected void minuReadLockCount() {
        while (true) {
            try {
                Stat lockPathStat = zk.exists(lockPath(), false);
                int readLockCount = getReadLockCount();
                Stat stat = zk.setData(lockPath(), String.valueOf(readLockCount - 1).getBytes(), lockPathStat.getVersion());
                if (stat.getVersion() - lockPathStat.getVersion() == 1) {
                    break;
                }
            } catch (Exception ignored) {

            }
        }
    }

    /**
     * 添加代表客户端的锁节点
     * @param childrenName
     * @return
     * @throws KeeperException
     * @throws InterruptedException
     */
    protected String addChildren(String childrenName) throws KeeperException, InterruptedException {
        String nodePath = zk.create(nodePath(childrenName), childrenName.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        return nodePath.split("/")[2];
    }


    /**
     * 获取锁资源路径下的子节点列表，并由小到大排序
     * @return
     * @throws KeeperException
     * @throws InterruptedException
     */
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


    /**
     * 尝试删除锁资源节点，可能会删除失败的
     */
    private void tryRemoveLockResource() {

        try {
            zk.delete(lockPath(), -1);
        } catch (Exception ignored) {

        }
    }


    /**
     * 关闭客户端连接并删除客户端代表的子节点
     * @throws KeeperException
     * @throws InterruptedException
     */
    protected void close() throws KeeperException, InterruptedException {
        reenTranLockCount = 0;
        zk.delete(nodePath(ownerLockName), -1);
        tryRemoveLockResource();
        zk.close();
    }


    /**
     * 有限时间监控目标节点的删除事件
     * @param previousNodeName
     * @param time
     * @param unit
     */
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


    /**
     * 有限时间监控锁资源节点的read_count的时间变化事件
     * @param time
     * @param unit
     * @throws KeeperException
     * @throws InterruptedException
     */
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


    /**
     * 阻塞式监控目标节点的删除事件
     * @throws KeeperException
     * @throws InterruptedException
     */
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
            readCountDataChangeSignal.await();
        }
    }

    /**
     * 阻塞式监控锁资源节点的read_count的时间变化事件
     * @param previousNodeName
     */
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


    /**
     * 判断本客户端是否已经获取过锁
     * @return
     */
    protected boolean isOwnerLock() {
        return hasLock;
    }

    /**
     * 占有锁
     * @param isOwnerLock
     */
    protected void setOwnerLock(boolean isOwnerLock) {
        hasLock = isOwnerLock;
        if (isOwnerLock) {
            reenTranLockCount = reenTranLockCount + 1;
        } else {
            reenTranLockCount = reenTranLockCount - 1;
        }
    }

}
