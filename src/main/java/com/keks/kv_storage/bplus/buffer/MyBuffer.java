package com.keks.kv_storage.bplus.buffer;

import com.keks.kv_storage.bplus.FreeSpaceCheckerInter;
import com.keks.kv_storage.bplus.page_manager.Page;
import com.keks.kv_storage.bplus.page_manager.PageIO;
import com.keks.kv_storage.bplus.page_manager.PageKey;
import com.keks.kv_storage.bplus.page_manager.page_key.PageType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;


public class MyBuffer {

    static final Logger log = LogManager.getLogger(MyBuffer.class.getName());

    private static final int LATCH_READ = 0;
    private static final int LATCH_WRITE = 1;
    private static final boolean TEST = false;

    public BufferStatistics buffStat = new BufferStatistics();
    private final ReentrantReadWriteLock lruFlushAllLock = new ReentrantReadWriteLock();

    private final PagesGroupLock[] pageGroupLocks; // needed to lock  while reading page from disk since 2 threads can try to read the same page from disk

    public final Map<PageKey, CachedPage<? extends Page>> lruCache = Collections.synchronizedMap(new LinkedHashMap<>());
    private final int bufferPoolSize;
    private volatile Integer bufCnt = 0;

    public MyBuffer(int bufferPoolSize) {
        this.bufferPoolSize = bufferPoolSize + 1;
        this.pageGroupLocks = new PagesGroupLock[this.bufferPoolSize];
        for (int i = 0; i < this.bufferPoolSize; i++) {
            pageGroupLocks[i] = new PagesGroupLock();
        }
    }

    public int getBufferCnt() {
        return bufCnt;
    }

    public ArrayList<Map.Entry<PageKey, CachedPage<? extends Page>>> getLruEntries() {
        return new ArrayList<>(lruCache.entrySet());
    }

    public <T extends Page> CachedPage<T> getAndLockWrite(PageKey key,
                                                          PageIO<T> pageReader) throws IOException {
        PagesGroupLock pageGroupLock = pageGroupLocks[(int) (key.pageId % bufferPoolSize)];
        return fix(pageGroupLock, key, LATCH_WRITE, pageReader);
    }

    public <T extends Page> CachedPage<T> getAndLockRead(PageKey key,
                                                         PageIO<T> pageReader) throws IOException {
        PagesGroupLock pageGroupLock = pageGroupLocks[(int) (key.pageId % bufferPoolSize)];
        return fix(pageGroupLock, key, LATCH_READ, pageReader);
    }

//    public <T extends Page, R extends Page> CachedPage<T> getAndLockRead2(PageKey key,
//                                                         PageIO<R> pageReader) throws IOException {
//        PagesGroupLock pageGroupLock = pageGroupLocks[(int) (key.pageId % bufferPoolSize)];
//        return fix2(pageGroupLock, key, LATCH_READ, pageReader);
//    }

    public AtomicInteger trySuccessCnt = new AtomicInteger();
    public AtomicInteger trySuccessButNullCnt = new AtomicInteger();
    public AtomicInteger tryFailCnt = new AtomicInteger();

    public <T extends Page> CachedPage<T> tryToGetCachedFreePage(FreeSpaceCheckerInter freeSpaceChecker,
                                                                 PageType pageType) {
        CachedPage<T> cachedPage = null;

        try {
            for (Map.Entry<PageKey, CachedPage<? extends Page>> entry : lruCache.entrySet()) {
                PageKey PageKey = entry.getKey();
                CachedPage<? extends Page> value = entry.getValue();
                if (PageKey.pageType == pageType && value.isNotFlushingOnDisk() && value.tryLock()) {
                    try {
                        if (value.isFlushingOnDisk() || value.isInUse() || value.page.isFull() || value.isFlushed) {

                        } else {
                            if (freeSpaceChecker.tryToTakePage(value.pageId)) {
                                printThread("Find page1 casting: " + value.pageId + " from cache" + " InUseCnt: " + value.getInUseCnt(), true);
                                cachedPage = value.cast();

                                cachedPage.incrementInUseCount();
                                printThread("Find page1: " + cachedPage.pageId + " from cache" + " InUseCnt: " + cachedPage.getInUseCnt(), true);
                                cachedPage.setDirty();
//                                cachedPage.sb.append("Thread[" + Thread.currentThread().getId() + "]  " + "PageId: " + cachedPage.pageId + " Read from buffer as free page finding\n");
                                break;
                            }
                        }
                    } finally {
                        value.unlock();
                    }
                }
            }

            if (cachedPage != null) {
                cachedPage.pageLockWrite();
                trySuccessCnt.incrementAndGet();
                printThread("Find page2: " + cachedPage.pageId + " from cache", true);
            } else {
                trySuccessButNullCnt.incrementAndGet();
            }
            buffStat.returnedFromBufferTryCnt.incrementAndGet();
            return cachedPage;
        } catch (ConcurrentModificationException e) {
            tryFailCnt.incrementAndGet();
            return null;
        }
    }

    private <T extends Page> CachedPage<T> fix(PagesGroupLock pageGroupLock,
                                               PageKey key,
                                               int latchMode,
                                               PageIO<T> pageReader) throws IOException {
        lruFlushAllLock.readLock().lock();
        try {
            CachedPage<T> page = getFromCacheOrDisk(pageGroupLock, key, latchMode, pageReader);
            assert page != null;
            if (latchMode == LATCH_WRITE) {
                page.pageLockWrite();
            } else {
                page.pageLockRead();
            }
            return page;
        } finally {
            lruFlushAllLock.readLock().unlock();
        }
    }

    private <T extends Page> CachedPage<T> getFromCacheOrDisk(PagesGroupLock pageGroupLock,
                                                              PageKey key,
                                                              int latchMode,
                                                              PageIO<T> pageReader) throws IOException {
        CachedPage<T> pageInCache = null;
        pageInCache = pullCacheWithReadLock(pageGroupLock, key, latchMode);
        if (pageInCache != null) {
            printThread("Read page: " + key.pageId + " from cache read lock" + " InUseCnt" + pageInCache.getInUseCnt(), true);
            return pageInCache;
        }
        return pullCacheWithWriteLockOrReadFromDisk(pageGroupLock, key, latchMode, pageReader);
    }

    private <T extends Page> CachedPage<T> pullCacheWithReadLock(PagesGroupLock pageGroupLock,
                                                                 PageKey key,
                                                                 int latchMode) {
        CachedPage<? extends Page> cachedPage;
        do {
            try {
                pageGroupLock.lockRead();
                cachedPage = lruCache.get(key);
                if (cachedPage == null) {
                    return null;
                } else {
                    cachedPage.lock();
                    try {
                        if (cachedPage.isNotFlushingOnDisk() && lruCache.containsKey(key)) {
                            cachedPage.incrementInUseCount();
                            if (latchMode == LATCH_WRITE) cachedPage.setDirty();
//                            cachedPage.sb.append("Thread[" + Thread.currentThread().getId() + "]  " + "PageId: " + cachedPage.pageId + " Read from buffer with read lock " + " InUseCnt" + cachedPage.getInUseCnt() + "\n");
                            updateCache(cachedPage);
                            return cachedPage.cast();
                        }
                    } finally {
                        cachedPage.unlock();
                    }
                }
            } finally {
                pageGroupLock.unlockRead();
            }
            waitIfBusy(cachedPage);
        } while (true);
    }

    private <T extends Page> CachedPage<T> pullCacheWithWriteLockOrReadFromDisk(PagesGroupLock pageGroupLock,
                                                                                PageKey key,
                                                                                int latchMode,
                                                                                PageIO<T> pageReader) throws IOException {
        CachedPage<? extends Page> cachedPage;
        do {
            try {
                pageGroupLock.lockWrite();
                cachedPage = lruCache.get(key);
                if (cachedPage == null) {
                    printThread("PageId: " + key.pageId + "  reading from disk", true);
                    CachedPage<T> pageFromDisk = readPageFromDisk(key, latchMode, pageReader);
                    pageFromDisk.incrementInUseCount();
                    buffStat.loadedFromDiskCnt.incrementAndGet();
//                    if (latchMode == LATCH_READ)
                    printThread("PageId: " + pageFromDisk.pageId + "  read from disk" + " InUseCnt" + pageFromDisk.getInUseCnt(), true);
//                    pageFromDisk.sb.append("Thread[" + Thread.currentThread().getId() + "]  " + "PageId: " + pageFromDisk.pageId + " Loaded from disk " + " InUseCnt" + pageFromDisk.getInUseCnt() + "\n");
                    addToCache(pageFromDisk);
                    return pageFromDisk;
                } else {
                    cachedPage.lock();
                    try {
                        if (cachedPage.isNotFlushingOnDisk() && lruCache.containsKey(key)) {
                            cachedPage.incrementInUseCount();
                            if (latchMode == LATCH_WRITE) cachedPage.setDirty();
                            printThread("Read page: " + key.pageId + " from cache write lock"+ " InUseCnt" + cachedPage.getInUseCnt(), true);
                            if (latchMode == LATCH_READ)
                                printThread("PageId: " + cachedPage.pageId + "  read from cache write" + " InUseCnt" + cachedPage.getInUseCnt(), true);
//                            cachedPage.sb.append("Thread[" + Thread.currentThread().getId() + "]  " + "PageId: " + cachedPage.pageId + " Read from buffer with write lock" + " InUseCnt" + cachedPage.getInUseCnt() + "\n");
                            updateCache(cachedPage);
                            return cachedPage.cast();
                        } else {
                            printThread("Waiting IO Page" + cachedPage.pageId);
                        }
                    } finally {
                        cachedPage.unlock();
                    }
                }
            } finally {
                pageGroupLock.unlockWrite();
            }
            waitIfBusy(cachedPage);
        } while (true);
    }

    public AtomicInteger treePageCnt = new AtomicInteger(0);
    public AtomicInteger indexPageCnt = new AtomicInteger(0);
    public AtomicInteger dataPageCnt = new AtomicInteger(0);

    // accessing only when leaf is locked on write
    private <T extends Page> CachedPage<T> readPageFromDisk(PageKey key, int latchMode, PageIO<T> pageReader) throws IOException {
        printThread("Read page: " + key.pageId + " from disk");
        T page = pageReader.getPage(key.pageId);
//        if (key.pageType == PageType.TREE_NODE_PAGE_TYPE) treePageCnt.incrementAndGet();
//        if (key.pageType == PageType.INDEX_PAGE_TYPE) indexPageCnt.incrementAndGet();
//        if (key.pageType == PageType.DATA_PAGE_TYPE) dataPageCnt.incrementAndGet();
        CachedPage<T> newCachedPage = new CachedPage<>(key, page, pageReader);
        if (latchMode == LATCH_WRITE) newCachedPage.setDirty();

        return newCachedPage;
    }

    public AtomicInteger updateCnt = new AtomicInteger(0);

    private <T extends Page> void updateCache(CachedPage<T> bcb) {
        lruCache.remove(bcb.key);
        buffStat.returnedFromBufferCnt.incrementAndGet();
        updateCnt.incrementAndGet();
        lruCache.put(bcb.key, bcb);
    }

    public AtomicInteger addCnt = new AtomicInteger(0);


    final Object obj = new Object();

    boolean print = true;
    private <T extends Page> void addToCache(CachedPage<T> newCachedPage) throws IOException {
        synchronized (bufCnt) {
            if (bufCnt < bufferPoolSize - 1) {
                bufCnt++;
                buffStat.pinedObjectsCnt.incrementAndGet();
                assert !lruCache.containsKey(newCachedPage.key);
                lruCache.put(newCachedPage.key, newCachedPage);
//                newCachedPage.sb.append("Thread[" + Thread.currentThread().getId() + "]  " + "PageId: " + newCachedPage.pageId + " Added to not full cache \n");
                return;
            }
        }

        CachedPage<? extends Page> victim = null;
        boolean doWrite = false;
        int cnt = 0;

        do {
            try {
                cnt++;
                if (cnt == Integer.MAX_VALUE) {
                    System.out.println("Cache is full. Cannot add new object. Increase cache size");
                    throw new RuntimeException("Cache is full. Cannot add new object. Increase cache size");
                }
                if (cnt == 10_000) {

                    synchronized (obj) {
                        if (print) {
                            print = false;
                            System.out.println("Cache is full");
                            for (Map.Entry<PageKey, CachedPage<? extends Page>> entry : lruCache.entrySet()) {
                                CachedPage<? extends Page> page = entry.getValue();
//                                System.out.println("PageId: " + page.pageId
//                                        + " is locked " + page.isLocked
//                                        + " InUseCnt: " + page.getInUseCnt()
//                                        + " is flushing " + page.isFlushingOnDisk()
//                                + " Is hold by Threads " + Arrays.toString(page.threads.toArray()));

                            }
                        }
                    }
                }




                for (Map.Entry<PageKey, CachedPage<? extends Page>> entry : lruCache.entrySet()) {
                    CachedPage<? extends Page> cachedPage = entry.getValue();
                    if (cachedPage.tryLock()) {
                        try {
                            if (cachedPage.isNotInUse() && cachedPage.isNotFlushingOnDisk() && !cachedPage.isFlushed) {
                                victim = cachedPage;
                                victim.setIsFlushingOnDisk();
//                                victim.sb.append("Thread[" + Thread.currentThread().getId() + "]  " + "PageId: " + victim.pageId + " Selected as victim \n"
//                                        + " is locked " + victim.isLocked
//                                        + " InUseCnt: " + victim.getInUseCnt()
//                                        + " is flushing " + victim.isFlushingOnDisk()
//                                        + " is Dirty " + victim.isDirty()
//                                        + " Is hold by Threads " + Arrays.toString(victim.threads.toArray()));
//                                printThread("PageId: " + victim.pageId + " Selected as victim \n"
//                                        + " is locked " + victim.isLocked
//                                        + " InUseCnt: " + victim.getInUseCnt()
//                                        + " is flushing " + victim.isFlushingOnDisk()
//                                        + " is Dirty " + victim.isDirty()
//                                        + " Is hold by Threads " + Arrays.toString(victim.threads.toArray()), true);
//                                if (cachedPage.isNotInUse() && cachedPage.threads.size() > 0) {
//
//                                    throw new RuntimeException("Thread[" + Thread.currentThread().getId() + "]  " + "VictimPageId: " + victim.pageId);
//                                }
                                if (victim.isDirty()) {
                                    doWrite = true;
                                }
                                break;
                            }
                        } finally {
                            cachedPage.unlock();
                        }
                    }
                }
            } catch (ConcurrentModificationException t) {

            }
        } while (victim == null);


        victim.lock();
        if (doWrite) {
            PageIO<? extends Page> pageIO = victim.pageIO;
            Page page = victim.page;

//            printThread("Flushed Victim page: " + page.pageId + " slots " + nextSlotId + " InUseCnt" + victim.getInUseCnt(), true);
//            victim.sb.append("Thread[" + Thread.currentThread().getId() + "]  " + "PageId: " + victim.pageId + " Flushed as victim " + " InUseCnt" + victim.getInUseCnt() + "\n");
            pageIO.flush(page);
        } else {
            Page page = victim.page;
//            victim.sb.append("Thread[" + Thread.currentThread().getId() + "]  " + "PageId: " + victim.pageId + " Not Flushed as victim " + "InUseCnt" + victim.getInUseCnt() + "\n");
//            printThread("Victim page doesn't need flush: " + victim.pageId + " slots " + ((SlottedPage) page).getNextSlotId() + " InUseCnt" + victim.getInUseCnt(), true);
        }

//        victim.sb.append("Thread[" + Thread.currentThread().getId() + "]  " + "PageId: " + victim.pageId + " Removing from cache " + "InUseCnt" + victim.getInUseCnt() + "\n");
        lruCache.remove(victim.key);
//        victim.sb.append("Thread[" + Thread.currentThread().getId() + "]  " + "PageId: " + victim.pageId + " Removed from cache " + "InUseCnt" + victim.getInUseCnt() + "\n");
        printThread("Removed Victim page: " + victim.pageId + " from cache" + " InUseCnt" + victim.getInUseCnt(), true);
        addCnt.incrementAndGet();
        victim.setCompletedFlushingOnDisk();
//        victim.sb.append("Thread[" + Thread.currentThread().getId() + "]  " + "PageId: " + victim.pageId + "Completed flushing on disk" + " InUseCnt" + victim.getInUseCnt() + " \n");
//        newCachedPage.sb.append("Thread[" + Thread.currentThread().getId() + "]  " + "PageId: " + newCachedPage.pageId + " Adding to cache " + "InUseCnt" + newCachedPage.getInUseCnt() + "\n");
        lruCache.put(newCachedPage.key, newCachedPage);
//        newCachedPage.sb.append("Thread[" + Thread.currentThread().getId() + "]  " + "PageId: " + newCachedPage.pageId + " Added to cache " + "InUseCnt" + newCachedPage.getInUseCnt() +"\n");
        victim.unlock();
    }

    private <T extends Page> void waitIfBusy(CachedPage<T> cachedPage) {
        cachedPage.lock();
        try {
            if (cachedPage.isFlushingOnDisk()) {
                cachedPage.awaitIOCompletion();
            } else {
                printThread("Not waiting IO  Page: " + cachedPage.pageId + "");
            }
        } finally {
            cachedPage.unlock();
        }
    }

    public <T extends Page> void unLock(CachedPage<T> cachedPage) throws IOException {
       unLock(cachedPage, false);
    }

    public <T extends Page> void unLock(CachedPage<T> cachedPage, boolean flush) throws IOException {
        cachedPage.lock();
        try {
            if (flush) {
                cachedPage.setIsFlushingOnDisk();
                PageIO<? extends Page> pageIO = cachedPage.pageIO;
                Page page = cachedPage.page;
                pageIO.flush(page);
                lruCache.remove(cachedPage.key);
                cachedPage.setCompletedFlushingOnDisk();
                synchronized (bufCnt) {
                    bufCnt--;
                }
            }
//            cachedPage.threads.remove(Thread.currentThread().getId());
            cachedPage.decrementInUseCount();
            printThread("Unlocking PageId: " + cachedPage.pageId + " InUseCnt: " + cachedPage.getInUseCnt(), true);
        } finally {
//            cachedPage.sb.append("Thread[" + Thread.currentThread().getId() + "]  " + "PageId: " + cachedPage.pageId + " Unlocking \n");
            cachedPage.unlock();
            cachedPage.unlockPage();
//            cachedPage.sb.append("Thread[" + Thread.currentThread().getId() + "]  " + "PageId: " + cachedPage.pageId + " Unlocked \n");
        }
    }

    public int flushAll() throws IOException {
        log.info("Flushing...");

        lruFlushAllLock.writeLock().lock();
        int cnt = 0;
        ArrayList<PageKey> keys = new ArrayList<>();
        lruCache.keySet().stream().iterator().forEachRemaining(keys::add);
        for (PageKey key : keys) {
            CachedPage<? extends Page> value = lruCache.get(key);
            if (value.isNotInUse()) {
                cnt++;
                value.pageIO.flush(value.getPage());
                lruCache.remove(key);
            }
        }
        bufCnt = bufCnt - cnt;
        buffStat.pinedObjectsCnt.set(bufCnt);
//        printThread("Flushed " + cnt + " of " + keys.size(), true);
        lruFlushAllLock.writeLock().unlock();
        return bufCnt;
    }

    public void flushAllForce() throws IOException {
        int remaining = flushAll();
        if (remaining > 0) throw new RuntimeException("Cannot flush all objects. Left: " + remaining);
    }

    static final class PagesGroupLock {

        private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

        void lockWrite() {
            lock.writeLock().lock();
        }

        void unlockWrite() {
            lock.writeLock().unlock();
        }

        void lockRead() {
            lock.readLock().lock();
        }

        void unlockRead() {
            lock.readLock().unlock();
        }
    }

    public static void printThread(String msg) {
        printThread(msg, false);
    }

    public static void printThread(String msg, boolean force) {
//        if (TEST || force) System.out.println("Thread[" + Thread.currentThread().getId() + "]  Buffer: " + msg);
//        logger.debug("Thread[" + Thread.currentThread().getId() + "]: " + msg);
    }


    static class BufferStatistics {

        public AtomicInteger pinedObjectsCnt = new AtomicInteger(0);
        public AtomicInteger loadedFromDiskCnt = new AtomicInteger(0);
        public AtomicInteger returnedFromBufferCnt = new AtomicInteger(0);
        public AtomicInteger returnedFromBufferTryCnt = new AtomicInteger(0);

        @Override
        public String toString() {
            final StringBuffer sb = new StringBuffer("BufferStatistics(");
            sb.append("pinedObjectsCnt=").append(pinedObjectsCnt);
            sb.append(", loadedFromDiskCnt=").append(loadedFromDiskCnt);
            sb.append(", returnedFromBufferCnt=").append(returnedFromBufferCnt);
            sb.append(", returnedFromBufferTryCnt=").append(returnedFromBufferTryCnt);
            sb.append(')');
            return sb.toString();
        }
    }

    public BufferStatistics getBuffStat() {
        return buffStat;
    }

}
