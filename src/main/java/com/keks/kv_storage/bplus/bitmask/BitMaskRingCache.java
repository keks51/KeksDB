package com.keks.kv_storage.bplus.bitmask;


import com.keks.kv_storage.bplus.FreeSpaceCheckerInter;
import com.keks.kv_storage.bplus.buffer.CachedPageNew;
import com.keks.kv_storage.bplus.buffer.CachedPageNew;
import com.keks.kv_storage.bplus.buffer.PageBuffer;
import com.keks.kv_storage.bplus.buffer.PageBuffer;
import com.keks.kv_storage.bplus.page_manager.Page;
import com.keks.kv_storage.bplus.page_manager.PageIO;
import com.keks.kv_storage.bplus.page_manager.page_key.PageKeyBuilder;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;


public class BitMaskRingCache<T extends Page> implements FreeSpaceCheckerInter {

//    public static final int CACHE_INIT_SIZE_DEFAULT = 10;

    public final int maxCacheSizeElems;
    public final int initCacheSize;
    private final PageBuffer buffer;

    private final PageIO<T> pageIO;
    private final PageKeyBuilder pageKeyBuilder;

    private final AtomicIntegerRoundRobin bucketRoundRobin;

    private final int nextBucketStep;

    private volatile long lastMaskId;

    private volatile long firstMaskId;

    private int currentCacheSize;

    private final AtomicLong setBits = new AtomicLong();

    public final ConcurrentRingCache<LongMaskConcurrent> cache;

    public BitMaskRingCache(int maxCacheElems,
                            int initCacheSize,
                            PageBuffer buffer,
                            PageIO<T> pageIO,
                            PageKeyBuilder pageKeyBuilder) throws IOException {
        if (maxCacheElems < initCacheSize)
            throw new IllegalArgumentException("maxCacheElems should be greater or equal then initCacheSize elems");
        this.maxCacheSizeElems = maxCacheElems;
        this.cache = new ConcurrentRingCache<>(maxCacheElems);
        this.buffer = buffer;
        this.pageIO = pageIO;
        this.pageKeyBuilder = pageKeyBuilder;

        this.bucketRoundRobin = new AtomicIntegerRoundRobin(Math.min(maxCacheElems, 64));
        this.nextBucketStep = 5;
        this.initCacheSize = initCacheSize;
        for (int i = 0; i < initCacheSize; i++) {
            LongMaskConcurrent mask = loadMask(i, true);
            cache.put(mask);
        }
        this.currentCacheSize = initCacheSize;
        this.lastMaskId = initCacheSize - 1;
    }

    private LongMaskConcurrent loadMask(long maskId, boolean flushLoadedTable) throws IOException {
        LongMaskConcurrent mask = new LongMaskConcurrent(maskId);
        int bit = 0;
        for (long pageId = maskId * 64L; pageId < (maskId + 1L) * 64; pageId++) {
            if (isPageFull(pageId, flushLoadedTable)) {
                mask.set(bit);
            }
            bit++;

        }
        return mask;
    }

    private boolean isPageFull(long pageId, boolean flushLoadedTable) throws IOException {
        CachedPageNew<T> cachedPage = buffer.getAndLockWrite(pageKeyBuilder.getPageKey(pageId), pageIO);
        try {
            return cachedPage.getPage().isFull();
        } finally {
            buffer.unLock(cachedPage, flushLoadedTable);
        }
    }

    // PT7.360613S
    private LongMaskConcurrent getFreeMask() {
        int startBucket = bucketRoundRobin.next();
        int loops = nextBucketStep;
//        int loops = rnd.nextInt(nextBucketStep + 1);
        for (int i = 0; i < loops; i++) {
            int nextBucket = startBucket;
            if (startBucket >= currentCacheSize) {
                startBucket = 0;
                nextBucket = 0;
            }
            for (; nextBucket < currentCacheSize; nextBucket = nextBucket + nextBucketStep) {
                LongMaskConcurrent bucket = cache.getElem(nextBucket);
                if (bucket.tryLockWrite()) {
                    if (!bucket.isFull()) {
                        return bucket;
                    } else {
                        bucket.unlockWrite();
                    }
                }
            }
            startBucket++;
        }
        return null;
    }

    private long tryNextFreePage() {
        LongMaskConcurrent freeBucket = getFreeMask();
        if (freeBucket == null) {
            return -1;
        } else {
            try {
                int firstUnsetFromRight = freeBucket.getFirstUnsetFromRight();
                long pageId = freeBucket.maskId * LongMask.BITS_IN_MASK + firstUnsetFromRight;
//                            System.out.println("MaksId: " + freeBucket.maskId + " PageId: " + pageId);
                setBits.incrementAndGet();
                return pageId;
            } finally {
                freeBucket.unlockWrite();
            }
        }
    }

    public long nextFreePage() throws IOException {
        long nextFreePageId = tryNextFreePage();
        if (nextFreePageId != -1) {
            return nextFreePageId;
        }

//

        synchronized (cache) {
            long freePageId = tryNextFreePage();
            while (freePageId == -1) {
                LongMaskConcurrent mask = loadMask(lastMaskId + 1, false);
//                System.out.println("Mask: " + mask.maskId + "was added to ring");
                cache.put(mask);
                lastMaskId++;
                if (currentCacheSize < maxCacheSizeElems) {
                    currentCacheSize++;
                } else {
                    firstMaskId++;
                }
                if (!mask.isFull()) {
                    freePageId = mask.maskId * LongMask.BITS_IN_MASK + mask.getFirstUnsetFromRight();
                    setBits.incrementAndGet();
                }
            }
            return freePageId;
        }
    }

    // 1 is free
    // 0 is not free
    // -1 page doesn't fit current mask range
    public int isFree(long pageId) {
        long maskId = pageId / LongMask.BITS_IN_MASK;
        int elemPos = (int) (maskId - firstMaskId);
        if (isMaskInRange(maskId) && elemPos >= 0 && elemPos < maxCacheSizeElems) {
            LongMaskConcurrent mask = cache.getElem(elemPos);

            if (mask.maskId == maskId) {
                int bitPos = (int) (pageId - maskId * LongMask.BITS_IN_MASK);
                return mask.isSet(bitPos) ? 0 : 1;
            }
        }
        return -1;
    }

    public boolean tryToTakePage(long pageId) {
        long maskId = pageId / LongMask.BITS_IN_MASK;
        int elemPos = (int) (maskId - firstMaskId);
        if (isMaskInRange(maskId) && elemPos >= 0 && elemPos < maxCacheSizeElems) {
            LongMaskConcurrent mask = cache.getElem(elemPos);
            if (mask.maskId == maskId) {
                int bitPos = (int) (pageId - maskId * LongMask.BITS_IN_MASK);
                boolean res = mask.tryToSet(bitPos);
                if (res) setBits.incrementAndGet();
                return res;
            }
        }
        return false;
    }

    public void setForceNotFree(long pageId) {
        long maskId = pageId / LongMask.BITS_IN_MASK;
        int elemPos = (int) (maskId - firstMaskId);
        if (isMaskInRange(maskId) && elemPos >= 0 && elemPos < maxCacheSizeElems) {
            LongMaskConcurrent mask = cache.getElem(elemPos);
            if (mask.maskId == maskId) {
                int bitPos = (int) (pageId - maskId * LongMask.BITS_IN_MASK);
                mask.set(bitPos);
                setBits.incrementAndGet();
            }
        }
    }

    public AtomicInteger missed = new AtomicInteger();

    public void setFree(long pageId) {
        long maskId = pageId / LongMask.BITS_IN_MASK;
        int elemPos = Math.abs((int) (cache.getElem(0).maskId - maskId));
        if (isMaskInRange(maskId) && elemPos >= 0 && elemPos < maxCacheSizeElems) {
            LongMaskConcurrent mask = cache.getElem(elemPos);
            if (mask.maskId == maskId) {
                int bitPos = (int) (pageId - maskId * LongMask.BITS_IN_MASK);
                if (mask.unSet(bitPos)) {
                    setBits.decrementAndGet();
                }
            } else {
                missed.incrementAndGet();
            }
        } else {
            missed.incrementAndGet();
        }
    }

    @Override
    public void close() throws IOException {

    }

    private boolean isMaskInRange(long maskId) {
        return (firstMaskId <= maskId) && (maskId <= lastMaskId);
    }

    public long getSetBitsCnt() {
        return setBits.get();
    }

    @Override
    public String toString() {
        String sb = "Ring contains " +
                currentCacheSize +
                " of max " +
                maxCacheSizeElems +
                "\n" +
                " BitsSet: " + setBits.get() +
                "\n" +
                cache;
        return sb;
    }

    public long getLastMaskId() {
        return lastMaskId;
    }

    public long getFirstMaskId() {
        return firstMaskId;
    }

    public double getAverageMaskLoad() {
//        double sum = 0.0;
//        for (int i = 0; i < currentCacheSize; i++) {
//            cache.
//        }
        double l = (double) (getLastMaskId() * 64);
        return l / setBits.get();
    }
}

