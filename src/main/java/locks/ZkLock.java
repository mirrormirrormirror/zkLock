package locks;

import org.apache.zookeeper.KeeperException;

import java.util.concurrent.TimeUnit;


/**
 * @author mirror
 */
public interface ZkLock {
    /**
     * 加锁方法 --- 阻塞
     * @throws KeeperException zookeeper 错误
     * @throws InterruptedException 线程中断错误
     */
    void lock() throws KeeperException, InterruptedException;

    /**
     * 加锁方法 --- 非阻塞
     * @return 加锁是否成功
     * @throws KeeperException zookeeper 错误
     * @throws InterruptedException 线程中断错误
     */
    boolean tryLock() throws KeeperException, InterruptedException;

    /**
     * 加锁方法 --- 非阻塞，有限时间等待
     * @param time
     * @param unit
     * @return
     * @throws InterruptedException
     * @throws KeeperException
     */
    boolean tryLock(long time, TimeUnit unit) throws InterruptedException, KeeperException;

    /**
     * 释放锁方法
     */
    void unlock() throws KeeperException, InterruptedException;

}
