# zkLock
利用zookeeper实现分布式锁，支持读写锁，可重入

### 使用方式 -- 相关依赖
``` pom
<repositories>
		<repository>
		    <id>jitpack.io</id>
		    <url>https://jitpack.io</url>
		</repository>
</repositories>
```
```
<dependency>
	    <groupId>com.github.mirrormirrormirror</groupId>
	    <artifactId>zkLock</artifactId>
	    <version>Tag</version>
</dependency>
```
### 例子
#### 公平读锁
``` java
        ZkReentrantReadWriteLock.ReadLock readLock = new ZkReentrantReadWriteLock("localhost", LOCK_TEST, true).readerLock();
        readLock.lock();
        System.out.println("一段逻辑");
        Thread.sleep(20000);
        readLock.unlock();
```
#### 非公平读锁（阻塞）
``` java
        ZkReentrantReadWriteLock.ReadLock readLock = new ZkReentrantReadWriteLock("localhost", LOCK_TEST, false).readerLock();
        readLock.lock();
        System.out.println("一段逻辑");
        Thread.sleep(20000);
        readLock.unlock();
```
#### 非公平读锁（非阻塞）
``` java
	ZkReentrantReadWriteLock.ReadLock readLock = new ZkReentrantReadWriteLock("localhost", "lock_test", false).readerLock();
        if (readLock.tryLock()) {
            System.out.println("一段逻辑");
            Thread.sleep(10000);
            readLock.unlock();
        }
``` 
#### 公平写锁
``` java
        ZkReentrantReadWriteLock.WriteLock writeLock = new ZkReentrantReadWriteLock("localhost", "lock_test", true).writeLock();
        writeLock.lock();
        System.out.println("一段逻辑");
        Thread.sleep(10000);
        writeLock.unlock();
```

#### 非公平写锁（阻塞）
``` java
        ZkReentrantReadWriteLock.WriteLock writeLock = new ZkReentrantReadWriteLock("localhost", "lock_test", false).writeLock();
        writeLock.lock();
        System.out.println("一段逻辑");
        Thread.sleep(10000);
        writeLock.unlock();
```
#### 非公平写锁（非阻塞）
``` java
        ZkReentrantReadWriteLock.WriteLock writeLock = new ZkReentrantReadWriteLock("localhost", "lock_test", false).writeLock();
        if (writeLock.tryLock()) {
            System.out.println("一段逻辑");
            Thread.sleep(10000);
            writeLock.unlock();
        }
```
