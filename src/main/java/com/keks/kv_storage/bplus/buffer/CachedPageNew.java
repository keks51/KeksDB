package com.keks.kv_storage.bplus.buffer;

import com.keks.kv_storage.bplus.page_manager.Page;
import com.keks.kv_storage.bplus.page_manager.PageIO;
import com.keks.kv_storage.bplus.page_manager.PageKey;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;


public class CachedPageNew<T extends Page> {

    public final PageKey key;
    public final long pageId;
    public final T page;
    public final PageIO<T> pageIO;
    private volatile long version;

    private volatile boolean dirty = false;

    public final ReentrantReadWriteLock pageLock = new ReentrantReadWriteLock();

    private volatile boolean isFlushed = false;

    public CachedPageNew(PageKey key, T page, PageIO<T> pageIO, long version) {
        this.key = key;
        this.pageId = key.pageId;
        this.page = page;
        this.pageIO = pageIO;
        this.version = version;
    }

    public void setVersion(long newVersion) {
        this.version = newVersion;
    }

    public long getVersion() {
        return version;
    }

    public final StringBuffer sb = new StringBuffer();

    public boolean isFlushed() {
        return isFlushed;
    }

    public void setFlushed() {
        isFlushed = true;
    }

    public boolean isDirty() {
        return dirty;
    }

    public void setDirty() {
        dirty = true;
    }


    private final AtomicInteger latchModeReadCnt = new AtomicInteger(0);
    private final AtomicInteger latchModeWriteCnt = new AtomicInteger(0);

    public void pageLockWrite() {
        pageLock.writeLock().lock();
        latchModeWriteCnt.incrementAndGet();
//        printThread("lock Write page " + key);
    }

    public boolean tryPageLockWrite() {
        if (!pageLock.isWriteLockedByCurrentThread() && pageLock.writeLock().tryLock()) {
            latchModeWriteCnt.incrementAndGet();
            return true;
        } else {
            return false;
        }
    }

    public void pageLockRead() {
        pageLock.readLock().lock();
        latchModeReadCnt.incrementAndGet();
//        printThread("lock Read page " + key);
    }

    public void unlockPage() {
        if (latchModeReadCnt.get() > 0) {
            latchModeReadCnt.decrementAndGet();
            pageLock.readLock().unlock();
//            printThread("Unlock Read page " + key);
            return;
        }
        if (latchModeWriteCnt.get() > 0) {
            latchModeWriteCnt.decrementAndGet();
            pageLock.writeLock().unlock();
//            printThread("Unlock Write page " + key);
            return;
        }
        throw new IllegalArgumentException("Cannot unlatch CachedPaged since no latch was applied");
    }

    public boolean isLockedWrite() {
        return pageLock.writeLock().isHeldByCurrentThread();
    }

    public T getPage() {
        return page;
    }

    public boolean isInUse() {
        return latchModeReadCnt.get() + latchModeWriteCnt.get() > 0;
    }

    public int getInUseCnt() {
        return latchModeReadCnt.get() + latchModeWriteCnt.get();
    }

    public <K extends Page> CachedPageNew<K> cast() {
        return (CachedPageNew<K>) this;
    }


    public static void printThread(String msg) {
        System.out.println("Thread[" + Thread.currentThread().getId() + "]  Buffer: " + msg);
    }
}
