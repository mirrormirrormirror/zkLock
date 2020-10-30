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

    private AbstractSync sync;
    private ReadLock readerLock;
    private WriteLock writerLock;


    public ZkReentrantReadWriteLock(String zkHost, int sessionTimeout, String resourceName) throws IOException, InterruptedException {
        initZkLock(zkHost, sessionTimeout, resourceName, false);
    }

    public ZkReentrantReadWriteLock(String zkHost, String resourceName) throws IOException, InterruptedException {
        initZkLock(zkHost, 60000, resourceName, false);
    }

    public ZkReentrantReadWriteLock(String zkHost, int sessionTimeout, String resourceName, boolean fair) throws IOException, InterruptedException {
        initZkLock(zkHost, sessionTimeout, resourceName, fair);
    }

    public ZkReentrantReadWriteLock(String zkHost, String resourceName, boolean fair) throws IOException, InterruptedException {
        initZkLock(zkHost, 60000, resourceName, fair);
    }

    private void initZkLock(String zkHost, int sessionTimeout, String resourceName, boolean fair) throws IOException, InterruptedException {
        sync = fair ? new FairSync(zkHost, sessionTimeout, resourceName) : new NonfairSync(zkHost, sessionTimeout, resourceName);
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
        private final AbstractSync sync;


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

        private final AbstractSync sync;

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


    /**
     * Nonfair version of Sync
     */
    static final class NonfairSync extends AbstractSync {
        NonfairSync(String zkHost, int sessionTimeout, String resourceName) throws IOException, InterruptedException {
            super(zkHost, sessionTimeout, resourceName);
        }

        @Override
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
                attemptNonFairLock(locks);
                if (isOwnerLock()) {
                    break;
                }
                int previousWatchNodeIndex = readerPreviousWatchNodeIndex(locks);
                if (previousWatchNodeIndex == -1) {
                    continue;
                }
                watchPreviousNode(locks.get(previousWatchNodeIndex));
            }
        }


        @Override
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
                attemptNonFairLock(locks);
                if (isOwnerLock()) {
                    return true;
                }
                int previousWatchNodeIndex = readerPreviousWatchNodeIndex(locks);
                if (previousWatchNodeIndex == -1) {
                    continue;
                }
                watchPreviousNode(locks.get(previousWatchNodeIndex), time, unit);
                nanosTimeout = deadline - System.nanoTime();
            }
            return isOwnerLock();
        }

        private void attemptNonFairLock(List<String> locks) throws KeeperException, InterruptedException {
            if (locks.get(0).equals(ownerLockName)) {
                startupAddReadLockCount();
                setOwnerLock(true);
            } else if (getReadLockCount() > 0 && attemptAddReadLockCount()) {
                setOwnerLock(true);
            }
        }


        @Override
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
            attemptNonFairLock(locks);

            if (isOwnerLock()) {
                return true;
            }
            return false;
        }
    }


    /**
     * Fair version of Sync
     */
    static final class FairSync extends AbstractSync {
        FairSync(String zkHost, int sessionTimeout, String resourceName) throws IOException, InterruptedException {
            super(zkHost, sessionTimeout, resourceName);
        }


        @Override
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
                attemptFairLock(locks);
                if (isOwnerLock()) {
                    break;
                }
                int previousWatchNodeIndex = readerPreviousWatchNodeIndex(locks);
                if (previousWatchNodeIndex == -1) {
                    continue;
                }
                watchPreviousNode(locks.get(previousWatchNodeIndex));
            }
        }

        private void attemptFairLock(List<String> locks) throws KeeperException, InterruptedException {
            if (locks.get(0).equals(ownerLockName)) {
                startupAddReadLockCount();
                setOwnerLock(true);
            } else if (getReadLockCount() > 0 && !betweenHead2ownerLockHasWriterLock(locks) && attemptAddReadLockCount()) {
                setOwnerLock(true);
            }
        }


        @Override
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
                attemptFairLock(locks);
                if (isOwnerLock()) {
                    return true;
                }
                int previousWatchNodeIndex = readerPreviousWatchNodeIndex(locks);
                if (previousWatchNodeIndex == -1) {
                    continue;
                }
                watchPreviousNode(locks.get(previousWatchNodeIndex), time, unit);
                nanosTimeout = deadline - System.nanoTime();
            }
            return isOwnerLock();
        }


        @Override
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
            attemptFairLock(locks);
            if (isOwnerLock()) {
                return true;
            }
            return false;
        }

    }

    abstract static class AbstractSync extends AbstractZkSynchronizer {

        private final String identifyId;

        protected String readerNodePrefix() {
            return READ_LOCK_PREFIX + identifyId + "_";
        }

        protected String writerNodePrefix() {
            return WRITE_LOCK_PREFIX + identifyId + "_";
        }

        AbstractSync(String zkHost, int sessionTimeout, String resourceName) throws IOException, InterruptedException {
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
                if (locks.get(0).equals(ownerLockName)) {
                    if (getReadLockCount() == 0) {
                        setOwnerLock(true);
                        break;
                    }
                    watchReadCount();
                    continue;
                }
                int previousWatchNodeIndex = writerPreviousWatchNodeIndex(locks);
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
                if (locks.get(0).equals(ownerLockName)) {
                    if (getReadLockCount() == 0) {
                        setOwnerLock(true);
                        break;
                    }
                    watchReadCount(i, unit);
                    continue;
                }
                int previousWatchNodeIndex = writerPreviousWatchNodeIndex(locks);
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

        public abstract void readLock(int i) throws KeeperException, InterruptedException;


        public abstract boolean tryReadLock(int i, long time, TimeUnit unit) throws KeeperException, InterruptedException;


        public abstract boolean tryReadLock() throws KeeperException, InterruptedException;


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
