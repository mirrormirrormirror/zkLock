package locks;

import org.apache.zookeeper.KeeperException;

import java.io.IOException;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * @author mirror
 */
public class ZkReentrantReadWriteLock {

    private Sync sync;
    private ReadLock readerLock;
    private WriteLock writerLock;


    public ZkReentrantReadWriteLock(String zkHost, int sessionTimeout, String resourceName) throws IOException, InterruptedException {
        initZkLock(zkHost, sessionTimeout, resourceName);
    }

    public ZkReentrantReadWriteLock(String zkHost, String resourceName) throws IOException, InterruptedException {
        initZkLock(zkHost, 60000, resourceName);
    }

    private void initZkLock(String zkHost, int sessionTimeout, String resourceName) throws IOException, InterruptedException {
        sync = new Sync(zkHost, sessionTimeout, resourceName);
        readerLock = new ReadLock(this);
        writerLock = new WriteLock(this);
    }


    public ZkReentrantReadWriteLock.WriteLock writeLock() {
        return writerLock;
    }

    public ZkReentrantReadWriteLock.ReadLock readerLock() {
        return readerLock;
    }

    public static class ReadLock implements ZkLock {
        private final Sync sync;


        public ReadLock(ZkReentrantReadWriteLock lock) {
            sync = lock.sync;
        }


        @Override
        public void lock() throws KeeperException, InterruptedException {
            sync.readLock(1);
        }


        @Override
        public boolean tryLock() throws KeeperException, InterruptedException {
            if (sync.tryReadLock()) {
                return true;
            } else {
                sync.close();
                return false;
            }
        }

        @Override
        public boolean tryLock(long time, TimeUnit unit) throws InterruptedException, KeeperException {
            if (sync.tryReadLock(1, time, unit)) {
                return true;
            } else {
                sync.close();
                return false;
            }
        }

        @Override
        public void unlock() throws KeeperException, InterruptedException {
            sync.release(1);
        }

    }


    public static class WriteLock implements ZkLock {

        private final Sync sync;

        WriteLock(ZkReentrantReadWriteLock lock) {
            sync = lock.sync;
        }

        @Override
        public void lock() throws KeeperException, InterruptedException {
            sync.acquire(1);
        }


        @Override
        public boolean tryLock() throws KeeperException, InterruptedException {
            if (sync.tryWriteLock()) {
                return true;
            } else {
                sync.close();
                return false;
            }
        }

        @Override
        public boolean tryLock(long time, TimeUnit unit) throws InterruptedException, KeeperException {
            if (sync.tryWriteLock(1, time, unit)) {
                return true;
            } else {
                sync.close();
                return false;
            }
        }

        @Override
        public void unlock() throws KeeperException, InterruptedException {
            sync.release(1);
        }

    }


    public static class Sync extends ZkSynchronizer {

        private final String identifyId;

        private String readerNodePrefix() {
            return READ_LOCK_PREFIX + identifyId + "_";
        }

        private String writerNodePrefix() {
            return WRITE_LOCK_PREFIX + identifyId + "_";
        }

        Sync(String zkHost, int sessionTimeout, String resourceName) throws IOException, InterruptedException {
            super(zkHost, sessionTimeout, resourceName);
            identifyId = UUID.randomUUID().toString().replaceAll("-", "");
        }


        public void acquire(int i) throws KeeperException, InterruptedException {
            if (isOwnerLock()) {
                addReenTranLock(i);
                return;
            }
            if (!existsLockPath()) {
                createNodeResource();
            }
            ownerLockName = addChildren(writerNodePrefix());

            while (!isOwnerLock()) {
                List<String> locks = getChildrenList();
                if (locks.get(0).equals(ownerLockName) && getReadLockCount() == 0) {
                    setOwnerLock(true);
                    break;
                }
                int previousWatchNodeIndex = writerPreviousWatchNodeIndex(locks, ownerLockName);
                if (previousWatchNodeIndex == -1) {
                    throw KeeperException.create(KeeperException.Code.SYSTEMERROR);
                }
                watchPreviousNode(locks.get(previousWatchNodeIndex));
            }
        }


        public boolean tryWriteLock(int i, long time, TimeUnit unit) throws KeeperException, InterruptedException {
            if (isOwnerLock()) {
                addReenTranLock(i);
                return true;
            }
            if (!existsLockPath()) {
                createNodeResource();
            }
            ownerLockName = addChildren(writerNodePrefix());
            long nanosTimeout = unit.toNanos(time);
            final long deadline = System.nanoTime() + nanosTimeout;
            while (!isOwnerLock() && nanosTimeout > 0L) {
                List<String> locks = getChildrenList();
                if (locks.get(0).equals(ownerLockName) && getReadLockCount() == 0) {
                    setOwnerLock(true);
                    return true;
                }
                int previousWatchNodeIndex = writerPreviousWatchNodeIndex(locks, ownerLockName);
                if (previousWatchNodeIndex == -1) {
                    throw KeeperException.create(KeeperException.Code.SYSTEMERROR);
                }
                watchPreviousNode(locks.get(previousWatchNodeIndex), time, unit);
                nanosTimeout = deadline - System.nanoTime();
            }
            return isOwnerLock();
        }


        public boolean tryWriteLock() throws KeeperException, InterruptedException {

            if (isOwnerLock()) {
                addReenTranLock(1);
                return true;
            }
            if (!existsLockPath()) {
                createNodeResource();
            }
            ownerLockName = addChildren(writerNodePrefix());
            List<String> locks = getChildrenList();
            if (locks.get(0).equals(ownerLockName) && getReadLockCount() == 0) {
                setOwnerLock(true);
                return true;
            }
            return false;
        }

        public void readLock(int i) throws KeeperException, InterruptedException {
            if (isOwnerLock()) {
                addReenTranLock(i);
                return;
            }
            if (!existsLockPath()) {
                createNodeResource();
            }
            ownerLockName = addChildren(readerNodePrefix());
            while (!isOwnerLock()) {
                List<String> locks = getChildrenList();
                if (locks.get(0).equals(ownerLockName) && startupAddReadLockCount()) {
                    setOwnerLock(true);
                } else if (getReadLockCount() > 0 && processAddReadLockCount()) {
                    setOwnerLock(true);
                }
                if (isOwnerLock()) {
                    break;
                }
                int previousWatchNodeIndex = readerPreviousWatchNodeIndex(locks);
                if (previousWatchNodeIndex == -1) {
                    throw KeeperException.create(KeeperException.Code.SYSTEMERROR);
                }
                watchPreviousNode(locks.get(previousWatchNodeIndex));
            }
        }


        public boolean tryReadLock(int i, long time, TimeUnit unit) throws KeeperException, InterruptedException {
            if (isOwnerLock()) {
                addReenTranLock(i);
                return true;
            }
            if (!existsLockPath()) {
                createNodeResource();
            }
            ownerLockName = addChildren(readerNodePrefix());
            long nanosTimeout = unit.toNanos(time);
            final long deadline = System.nanoTime() + nanosTimeout;
            while (!isOwnerLock() && nanosTimeout > 0L) {
                List<String> locks = getChildrenList();
                if (locks.get(0).equals(ownerLockName) && startupAddReadLockCount()) {
                    setOwnerLock(true);
                } else if (getReadLockCount() > 0 && processAddReadLockCount()) {
                    setOwnerLock(true);
                }
                if (isOwnerLock()) {
                    return true;
                }
                int previousWatchNodeIndex = readerPreviousWatchNodeIndex(locks);
                if (previousWatchNodeIndex == -1) {
                    throw KeeperException.create(KeeperException.Code.SYSTEMERROR);
                }
                watchPreviousNode(locks.get(previousWatchNodeIndex), time, unit);
                nanosTimeout = deadline - System.nanoTime();
            }
            return isOwnerLock();
        }


        public boolean tryReadLock() throws KeeperException, InterruptedException {
            if (isOwnerLock()) {
                addReenTranLock(1);
                return true;
            }
            if (!existsLockPath()) {
                createNodeResource();
            }
            ownerLockName = addChildren(readerNodePrefix());
            List<String> locks = getChildrenList();
            if (locks.get(0).equals(ownerLockName) && startupAddReadLockCount()) {
                setOwnerLock(true);
            } else if (getReadLockCount() > 0 && processAddReadLockCount()) {
                setOwnerLock(true);
            }

            if (isOwnerLock()) {
                return true;
            }
            return false;
        }

        public void release(int i) throws KeeperException, InterruptedException {
            if (ownerLockName.contains(READ_LOCK_PREFIX)) {
                minuReadLockCount();
            }
            if (getReenTranLockCount() > 1) {
                minuReenTranLock(i);
            } else {
                setOwnerLock(false);
                close();
            }
        }
    }
}
