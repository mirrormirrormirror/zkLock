import locks.ZkReentrantReadWriteLock;
import org.apache.zookeeper.KeeperException;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Create by mirror on 2020/10/24
 */
public class Test {


    public static void main(String[] args) {
        ReentrantReadWriteLock.ReadLock readLock = new ReentrantReadWriteLock(true).readLock();
    }

    @org.junit.Test
    public void testTryReadLock() throws KeeperException, InterruptedException, IOException {
        ZkReentrantReadWriteLock.ReadLock readLock = new ZkReentrantReadWriteLock("localhost", "mirror_read", true).readerLock();
        if (readLock.tryLock()) {
            System.out.println("一段逻辑");
            Thread.sleep(20000);
            readLock.unlock();
        }
    }

    @org.junit.Test
    public void testReadLock() throws KeeperException, InterruptedException, IOException {
        ZkReentrantReadWriteLock.ReadLock readLock = new ZkReentrantReadWriteLock("localhost", "mirror_read").readerLock();
        readLock.lock();
        System.out.println("一段逻辑");
        Thread.sleep(20000);
        readLock.unlock();
    }

    @org.junit.Test
    public void testWriteTryLock() throws KeeperException, InterruptedException, IOException {
        ZkReentrantReadWriteLock.WriteLock writeLock = new ZkReentrantReadWriteLock("localhost", "mirror_read").writeLock();
        if (writeLock.tryLock()) {
            System.out.println("一段逻辑");
            Thread.sleep(20000);
            writeLock.unlock();
        }
    }

    @org.junit.Test
    public void testWriteLock() throws KeeperException, InterruptedException, IOException {
        ZkReentrantReadWriteLock.WriteLock writeLock = new ZkReentrantReadWriteLock("localhost", "mirror_read").writeLock();
        writeLock.lock();
        System.out.println("一段逻辑");
        Thread.sleep(20000);
        writeLock.unlock();
    }


    @org.junit.Test
    public void testWriteTryTimeoutLock() throws KeeperException, InterruptedException, IOException {
        ZkReentrantReadWriteLock.WriteLock writeLock = new ZkReentrantReadWriteLock("localhost", "mirror_try_timeout").writeLock();
        if (writeLock.tryLock(3, TimeUnit.SECONDS)) {
            System.out.println("一段逻辑");
            Thread.sleep(10000);
            writeLock.unlock();
        }
    }

    @org.junit.Test
    public void testReadTryTimeoutLock() throws KeeperException, InterruptedException, IOException {
        ZkReentrantReadWriteLock.ReadLock readLock = new ZkReentrantReadWriteLock("localhost", "mirror_try_timeout").readerLock();
        if (readLock.tryLock(15, TimeUnit.SECONDS)) {
            System.out.println("一段逻辑");
            Thread.sleep(10000);
            readLock.unlock();
        }
    }


    @org.junit.Test
    public void testReadReentranLock() throws KeeperException, InterruptedException, IOException {
        ZkReentrantReadWriteLock.ReadLock readLock = new ZkReentrantReadWriteLock("localhost", "mirror_try_timeout").readerLock();

        readLock.lock();
        System.out.println("第一段逻辑---");
        Thread.sleep(5000);
        readLock.lock();
        System.out.println("第二段逻辑---");
        Thread.sleep(5000);
        readLock.unlock();
        System.out.println("释放第二把锁");
        Thread.sleep(5000);
        readLock.unlock();
        System.out.println("释放第一把锁");


    }


}
