package com.keks.kv_storage.bplus.buffer;

import com.keks.kv_storage.bplus.page_manager.Page;
import com.keks.kv_storage.bplus.page_manager.PageIO;
import com.keks.kv_storage.bplus.page_manager.PageKey;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class CachedPage<T extends Page> {

    public final List<Long> threads = Collections.synchronizedList(new LinkedList<>());

    public final PageKey key;
    public final long pageId;
    public final T page;
    public final PageIO<T> pageIO;


    private final AtomicInteger inUseCnt = new AtomicInteger(0);
    private volatile boolean isFlushingOnDisk = false;
    private volatile boolean dirty = false;
    private volatile boolean isLocked = false;

    private final Lock lock = new ReentrantLock();
    public final ReentrantReadWriteLock pageLock = new ReentrantReadWriteLock();
    private final Condition ioDone = lock.newCondition();

    public volatile boolean isFlushed = false;

    public CachedPage(PageKey key, T page, PageIO<T> pageIO) {
        this.key = key;
        this.pageId = key.pageId;
        this.page = page;
        this.pageIO = pageIO;
    }

    public boolean tryLock() {
        boolean b = lock.tryLock();
        isLocked = b;
        return b;
    }

    public final StringBuffer sb = new StringBuffer();

    public void lock() {
        lock.lock();
        isLocked = true;
    }

    public void unlock() {
        lock.unlock();
        isLocked = false;
    }

    public boolean isFlushed() {
        return isFlushed;
    }

    public boolean isFlushingOnDisk() {
        return isFlushingOnDisk;
    }

    public boolean isNotFlushingOnDisk() {
        return !isFlushingOnDisk;
    }

    public void setIsFlushingOnDisk() {
        isFlushingOnDisk = true;
    }

    public void setCompletedFlushingOnDisk() {
        isFlushingOnDisk = false;
        isFlushed = true;
        ioDone.signalAll();
    }

    public void incrementInUseCount() {
        inUseCnt.incrementAndGet();
    }

    public void decrementInUseCount() {
        inUseCnt.decrementAndGet();
    }

    public boolean isInUse() {
        return inUseCnt.get() > 0;
    }

    public boolean isNotInUse() {
        return inUseCnt.get() == 0;
    }

    public int getInUseCnt() {
        return inUseCnt.get();
    }

    public boolean isDirty() {
        return dirty;
    }

    public void awaitIOCompletion() {
        try {
            boolean await = ioDone.await(1, TimeUnit.SECONDS);
        } catch (InterruptedException ignored) {
        }
    }

    public void setDirty() {
        dirty = true;
    }


    private final AtomicInteger latchModeReadCnt = new AtomicInteger(0);
    private final AtomicInteger latchModeWriteCnt = new AtomicInteger(0);

    public void pageLockWrite() {
        pageLock.writeLock().lock();
        latchModeWriteCnt.incrementAndGet();
    }

    public void pageLockRead() {
        pageLock.readLock().lock();
        latchModeReadCnt.incrementAndGet();
    }

    public void unlockPage() {
        if (latchModeReadCnt.get() > 0) {
            latchModeReadCnt.decrementAndGet();
            pageLock.readLock().unlock();
//                printThread("Unlock Read page " + pageId, true);
            return;
        }
        if (latchModeWriteCnt.get() > 0) {
            latchModeWriteCnt.decrementAndGet();
            pageLock.writeLock().unlock();
//                printThread("Unlock Write page " + pageId, true);
            return;
        }
        throw new IllegalArgumentException("Cannot unlatch CachedPaged since no latch was applied");
    }

    public T getPage() {
        return page;
    }

    public <K extends Page> CachedPage<K>  cast() {
        return (CachedPage<K>) this;
    }


}