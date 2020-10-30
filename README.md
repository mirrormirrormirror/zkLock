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
#### 读锁
``` java
        ZkReentrantReadWriteLock.ReadLock readLock = new ZkReentrantReadWriteLock("localhost", LOCK_TEST, true).readerLock();
        readLock.lock();
        System.out.println("一段逻辑");
        Thread.sleep(20000);
        readLock.unlock();
```

#### 写锁
``` java
        ZkReentrantReadWriteLock.WriteLock writeLock = new ZkReentrantReadWriteLock("localhost", "lock_test", true).writeLock();
        writeLock.lock();
        System.out.println("一段逻辑");
        Thread.sleep(10000);
        writeLock.unlock();
```
