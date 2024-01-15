package com.keks.kv_storage.bplus.buffer;

import com.keks.kv_storage.bplus.page_manager.Page;
import com.keks.kv_storage.bplus.page_manager.PageIO;
import com.keks.kv_storage.bplus.page_manager.PageKey;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.ConcurrentModificationException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;


public class PageBuffer {

    volatile boolean isFull = false;
    static final Logger log = LogManager.getLogger(PageBuffer.class.getName());

    public AtomicLong version = new AtomicLong(0);

    private static final int LATCH_READ = 0;
    private static final int LATCH_WRITE = 1;

    public volatile Integer bufCnt = 0;

    //    public final Map<PageKey, CachedPageNew<? extends Page>> lruCache = Collections.synchronizedMap(new LinkedHashMap<>());
    public final Map<PageKey, CachedPageNew<? extends Page>> lruCache = new ConcurrentHashMap<>(50_000, 0.75f, 200);

    private final ReentrantReadWriteLock flushAllLock = new ReentrantReadWriteLock();

    private final int bufferPoolSize;
    private final PagesGroupLock[] pageGroupLocks;

    public PageBuffer(int bufferPoolSize) {
        this.bufferPoolSize = bufferPoolSize + 1;
        this.pageGroupLocks = new PagesGroupLock[this.bufferPoolSize];
        for (int i = 0; i < this.bufferPoolSize; i++) {
            pageGroupLocks[i] = new PagesGroupLock();
        }
    }

    public <T extends Page> CachedPageNew<T> getAndLockWrite(PageKey key,
                                                             PageIO<T> pageReader) throws IOException {

        return fix(key, LATCH_WRITE, pageReader);
    }

    public <T extends Page> CachedPageNew<T> getAndLockRead(PageKey key,
                                                            PageIO<T> pageReader) throws IOException {
        return fix(key, LATCH_READ, pageReader);
    }

    private <T extends Page> CachedPageNew<T> fix(PageKey key,
                                                  int latchMode,
                                                  PageIO<T> pageReader) throws IOException {
        flushAllLock.readLock().lock();
        try {
            CachedPageNew<T> page = getFromCacheOrDisk(key, latchMode, pageReader);
            return page;
        } finally {
            flushAllLock.readLock().unlock();
        }
    }

    private <T extends Page> CachedPageNew<T> getFromCacheOrDisk(PageKey key,
                                                                 int latchMode,
                                                                 PageIO<T> pageReader) throws IOException {
        CachedPageNew<T> pageInCache = null;
//        PagesGroupLock pageGroupLock = pageGroupLocks[(int) (key.pageId % bufferPoolSize)];
//        pageGroupLock.lockRead();
        pageInCache = pullCache(key, latchMode);
//        pageGroupLock.unlockRead();
        if (pageInCache != null) {
            return pageInCache;
        }
        return pullCacheWithWriteLockOrReadFromDisk(key, latchMode, pageReader);
    }

//    public static Timer pullCacheOuter = BPlusKVTable.registry.timer("pullCacheOuter");
//    public static Timer pullCache = BPlusKVTable.registry.timer("pullCache");
//    public static Timer pullCache2 = BPlusKVTable.registry.timer("pullCache2");
//    public static Timer lockReadPage = BPlusKVTable.registry.timer("lockReadPage");
//    public static Timer updateCache = BPlusKVTable.registry.timer("updateCache");
//    public static Timer castPage = BPlusKVTable.registry.timer("castPage");

//    private <T extends Page> CachedPageNew<T> pullCache2(PageKey key,
//                                                         int latchMode) {
//        CachedPageNew<? extends Page> cachedPage = null;
//
//        cachedPage = Time.withTimer(pullCache, () -> lruCache.get(key));
//        CachedPageNew<? extends Page> finalCachedPage2 = cachedPage;
//        return Time.withTimer(pullCache2, () -> {
//            if (finalCachedPage2 == null) {
//                if (Time.mes) System.out.println("here1241");
//                return null;
//            } else {
//                if (latchMode == LATCH_READ) {
//                    CachedPageNew<? extends Page> finalCachedPage1 = finalCachedPage2;
//                    Time.withTimer(lockReadPage, () -> finalCachedPage1.pageLockRead());
//                } else {
//                    if (Time.mes) System.out.println("here1246");
//                    finalCachedPage2.pageLockWrite();
//                    finalCachedPage2.setDirty();
//                }
//                if (finalCachedPage2.isFlushed()) {
//                    if (Time.mes) System.out.println("here1245");
//                    finalCachedPage2.unlockPage();
//                    return null;
//                } else {
//                    CachedPageNew<? extends Page> finalCachedPage = finalCachedPage2;
//                    Time.withTimer(updateCache, () -> updateCache(finalCachedPage));
//                    return Time.withTimer(castPage, () -> finalCachedPage.cast());
//                }
//            }
//        });
//
//
//    }

    private <T extends Page> CachedPageNew<T> pullCache(PageKey key,
                                                        int latchMode) {
        CachedPageNew<? extends Page> cachedPage = null;

        cachedPage = lruCache.get(key);


        if (cachedPage == null) {
            return null;
        } else {
            if (latchMode == LATCH_READ) {
                cachedPage.pageLockRead();
            } else {
                cachedPage.pageLockWrite();
                cachedPage.setDirty();
            }
            if (cachedPage.isFlushed()) {
                cachedPage.unlockPage();
                return null;
            } else {
                updateCache(cachedPage);
                return cachedPage.cast();
            }
        }


    }

    private <T extends Page> CachedPageNew<T> pullCacheWithWriteLockOrReadFromDisk(PageKey key,
                                                                                   int latchMode,
                                                                                   PageIO<T> pageReader) throws IOException {
        CachedPageNew<? extends Page> cachedPage;
        PagesGroupLock pageGroupLock = pageGroupLocks[(int) (key.pageId % bufferPoolSize)];
        try {
            pageGroupLock.lockWrite();
            cachedPage = pullCache(key, latchMode);
            if (cachedPage == null) {
                CachedPageNew<T> pageFromDisk = readPageFromDisk(key, pageReader);

                if (latchMode == LATCH_READ) {
                    pageFromDisk.pageLockRead();
                } else {
                    pageFromDisk.setDirty();
                    pageFromDisk.pageLockWrite();
                }
                addToCache(pageFromDisk);
                return pageFromDisk;
            } else {
                return cachedPage.cast();
            }

        } finally {
            pageGroupLock.unlockWrite();
        }

    }

    private <T extends Page> CachedPageNew<T> readPageFromDisk(PageKey key,
                                                               PageIO<T> pageReader) throws IOException {
        T page = pageReader.getPage(key.pageId);
        CachedPageNew<T> newCachedPage = new CachedPageNew<>(key, page, pageReader, version.incrementAndGet());
        return newCachedPage;
    }

    public static AtomicInteger cnt1 = new AtomicInteger(0);
    public static AtomicInteger cnt2 = new AtomicInteger(0);
    public static AtomicInteger cnt3 = new AtomicInteger(0);

    private <T extends Page> void updateCache(CachedPageNew<T> bcb) {
//        cnt1.incrementAndGet();
//        long diff = version.get() - bcb.getVersion();
//        if (diff > bufferPoolSize * 0.75 && bufCnt > bufferPoolSize * 0.75) {
//            PagesGroupLock pageGroupLock = pageGroupLocks[(int) (bcb.pageId % bufferPoolSize)];
//            cnt2.incrementAndGet();
//            if (pageGroupLock.tryLockWrite()) {
//                if (version.get() - bcb.getVersion() > bufferPoolSize * 0.75) {
//                    cnt3.incrementAndGet();
//                    lruCache.remove(bcb.key);
//                    bcb.setVersion(version.incrementAndGet());
//                    lruCache.put(bcb.key, bcb);
//                }
//                pageGroupLock.unlockWrite();
//            }
//
//        }
    }

    public static SimpleMeterRegistry registry = new SimpleMeterRegistry();
    public static Timer victimsStepsToFind = registry.timer("victimsStepsToFind");


    private <T extends Page> void addToCache(CachedPageNew<T> newCachedPage) throws IOException {
        synchronized (bufCnt) { // TODO test does sync is needed here. currently is used
            if (bufCnt < bufferPoolSize - 1) {
                bufCnt++;
                assert !lruCache.containsKey(newCachedPage.key);
                lruCache.put(newCachedPage.key, newCachedPage);
//                queue.add(newCachedPage);
                return;
            }
        }
//        System.out.println("here126");
        CachedPageNew<? extends Page> victim = null;
        boolean doWrite = false;
        int cnt = 0;

        do {
            try {
                cnt++;
                if (cnt == Integer.MAX_VALUE) {
                    System.out.println("Cache is full. Cannot add new object. Increase cache size");
                    throw new RuntimeException("Cache is full. Cannot add new object. Increase cache size");
                }

                int steps = 0;
                for (Map.Entry<PageKey, CachedPageNew<? extends Page>> entry : lruCache.entrySet()) {
                    steps++;
                    CachedPageNew<? extends Page> cachedPage = entry.getValue();
                    if (cachedPage.tryPageLockWrite()) {
                        if (cachedPage.isFlushed()) {
                            cachedPage.unlockPage();
                        } else {
                            victimsStepsToFind.record(steps, TimeUnit.SECONDS);
                            victim = cachedPage;
                            victim.setFlushed();
                            if (victim.isDirty()) {
                                doWrite = true;
                            }
                            break;
                        }
                    }
                }
            } catch (ConcurrentModificationException ignored) {
            }
        } while (victim == null);


        if (doWrite) {
            PageIO<? extends Page> pageIO = victim.pageIO;
            Page page = victim.page;
            pageIO.flush(page);
//            printThread("Page: " + victim.key + " was flushed");

        }

        lruCache.remove(victim.key);
//        queue.remove(victim);
        victim.unlockPage();
        lruCache.put(newCachedPage.key, newCachedPage);
//        printThread("Page: " + newCachedPage.key + " was added to cache");
    }

    public <T extends Page> void unLock(CachedPageNew<T> cachedPage) throws IOException {
        unLock(cachedPage, false);
    }

    public <T extends Page> void unLock(CachedPageNew<T> cachedPage, boolean flush) throws IOException {
        try {
            if (flush) {
                assert cachedPage.isLockedWrite();
                cachedPage.setFlushed();
                PageIO<? extends Page> pageIO = cachedPage.pageIO;
                Page page = cachedPage.page;
                pageIO.flush(page);
//                queue.remove(cachedPage);
                lruCache.remove(cachedPage.key);
                synchronized (bufCnt) {
                    bufCnt--;
                }
            }
        } finally {
            cachedPage.unlockPage();
        }
    }

    public int flushAll() throws IOException {
        log.info("Flushing...");
        flushAllLock.writeLock().lock();
        int cnt = 0;
        ArrayList<PageKey> keys = new ArrayList<>();
        lruCache.keySet().stream().iterator().forEachRemaining(keys::add);
        for (PageKey key : keys) {
            CachedPageNew<? extends Page> value = lruCache.get(key);
            if (value.tryPageLockWrite()) {
                cnt++;
                value.pageIO.flush(value.getPage());
                lruCache.remove(key);
                value.unlockPage();
            }
        }
        bufCnt = bufCnt - cnt;

        flushAllLock.writeLock().unlock();
        return bufCnt;
    }

    public void flushAllForce() throws IOException {
        int remaining = flushAll();
        if (remaining > 0) throw new RuntimeException("Cannot flush all objects. Left: " + remaining);
    }

    public static void printThread(String msg) {
        System.out.println("Thread[" + Thread.currentThread().getId() + "]  Buffer: " + msg);
    }

}


//    public void calcNodes() throws IOException {
//        StringBuilder sb = new StringBuilder();
//        if (treeNodePageManager.bplusTreeRuntimeParameters.rootExist()) {
//            CachedPageNew<TreeNodePage> page = treeNodePageManager.getAndLockReadPage(treeNodePageManager.bplusTreeRuntimeParameters.getRootPageId());
//            long mostLeftPageId = TreeKeyNode.apply(page.getPage(), treeNodePageManager.bPlusEngineConf.btreeConf).keysAndChildren.getLeftChild(0);
//            treeNodePageManager.unlockPage(page);
//            sb.append("Root: 1\n");
//            calcNodes(mostLeftPageId, sb);
//
//        } else {
//            long mostLeftLeafPageId = treeNodePageManager.bplusTreeRuntimeParameters.getMostLeftLeafPageId();
//            if (mostLeftLeafPageId > 0) {
//                printLeaf(treeNodePageManager.bplusTreeRuntimeParameters.getMostLeftLeafPageId(), sb);
//            } else {
//                System.out.println("null");
//            }
//        }
//        System.out.println(sb);
//    }
//
//    private void calcNodes(long pageId, StringBuilder sb) throws IOException {
//        CachedPageNew<TreeNodePage> page = treeNodePageManager.getAndLockReadPage(pageId);
//        if (page.getPage().isLeafNode()) {
//            treeNodePageManager.unlockPage(page);
//            calcLeaf(page.pageId, sb);
//        } else {
//            treeNodePageManager.unlockPage(page);
//            calcKey(pageId, sb);
//            long mostLeftPageId = TreeKeyNode.apply(page.getPage(), treeNodePageManager.bPlusEngineConf.btreeConf).keysAndChildren.getLeftChild(0);
//
//            calcNodes(mostLeftPageId, sb);
//        }
//    }
//
//

//    private void calcLeaf(long pageId, StringBuilder sb) throws IOException {
//
//        long curPageId = pageId;
//        CachedPageNew<TreeNodePage> page;
//        TreeLeafNode current;
//        int cnt = 0;
//        do {
//            cnt++;
//            page = treeNodePageManager.getAndLockReadPage(curPageId);
//            current = TreeLeafNode.apply(page.getPage(), treeNodePageManager.bPlusEngineConf.btreeConf);
//            curPageId = current.getRightSibling();
//            treeNodePageManager.unlockPage(page);
//        } while (current.getRightSibling() != 0);
//        sb.append("Leaf: " + cnt + "\n");
//    }


//    private void calcKey(long pageId, StringBuilder sb) throws IOException {
//        long curPageId = pageId;
//        CachedPageNew<TreeNodePage> page;
//        TreeKeyNode current;
//        int cnt = 0;
//        do {
//            cnt++;
//            page = treeNodePageManager.getAndLockReadPage(curPageId);
//            current = TreeKeyNode.apply(page.getPage(), treeNodePageManager.bPlusEngineConf.btreeConf);
//            curPageId = current.getRightSibling();
//            treeNodePageManager.unlockPage(page);
//        } while (curPageId != 0);
//        sb.append("KeyNodes: " + cnt + "\n");
//    }
