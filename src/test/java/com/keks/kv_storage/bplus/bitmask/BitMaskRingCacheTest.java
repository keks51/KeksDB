package com.keks.kv_storage.bplus.bitmask;

import com.keks.kv_storage.bplus.buffer.PageBuffer;
import com.keks.kv_storage.bplus.page_manager.page_key.PageKeyBuilder;
import com.keks.kv_storage.bplus.page_manager.page_key.PageType;
import com.keks.kv_storage.bplus.page_manager.page_disk.sp.SlottedPage;
import com.keks.kv_storage.bplus.page_manager.pageio.SlottedPageIO;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static com.keks.kv_storage.bplus.bitmask.LongMask.BITS_IN_MASK;
import static org.junit.jupiter.api.Assertions.assertEquals;


class BitMaskRingCacheTest {


    @Test
    public void test1(@TempDir Path dir) throws IOException {
        PageBuffer myBuffer = new PageBuffer(5);
        SlottedPageIO pageIO = new SlottedPageIO(dir.toFile(), "test.db");
        PageKeyBuilder pageKeyBuilder = new PageKeyBuilder(PageType.DATA_PAGE_TYPE);
        BitMaskRingCache<SlottedPage> ringCache = new BitMaskRingCache<>(10, 10, myBuffer, pageIO, pageKeyBuilder);


        for (int i = 0; i < 10 * BITS_IN_MASK; i++) {
            ringCache.nextFreePage();
        }
//        System.out.println(ringCache);

        assert (10L * BITS_IN_MASK == ringCache.getSetBitsCnt());
    }

    @Test
    public void test2(@TempDir Path dir) throws IOException {
        PageBuffer myBuffer = new PageBuffer(5);
        SlottedPageIO pageIO = new SlottedPageIO(dir.toFile(), "test.db");
        PageKeyBuilder pageKeyBuilder = new PageKeyBuilder(PageType.DATA_PAGE_TYPE);
        int maxCacheElems = 15;
        int masksCnt = 20;
        BitMaskRingCache<SlottedPage> ringCache = new BitMaskRingCache<>(maxCacheElems, 10, myBuffer, pageIO, pageKeyBuilder);

        for (int i = 0; i < masksCnt * BITS_IN_MASK; i++) {
            ringCache.nextFreePage();
        }

//        System.out.println("res");
//        System.out.println(masksCnt * BITS_IN_MASK);
//        System.out.println(ringCache.getSetBitsCnt());
//        System.out.println(ringCache);
        assert (((long) masksCnt ) * BITS_IN_MASK == ringCache.getSetBitsCnt());
    }

    @Test
    public void test3(@TempDir Path dir) throws IOException {
        PageBuffer myBuffer = new PageBuffer(5);
        SlottedPageIO pageIO = new SlottedPageIO(dir.toFile(), "test.db");
        PageKeyBuilder pageKeyBuilder = new PageKeyBuilder(PageType.DATA_PAGE_TYPE);
        int maxCacheElems = 128;
        int masksCnt = 129;
        BitMaskRingCache<SlottedPage> ringCache = new BitMaskRingCache<>(maxCacheElems, 10, myBuffer, pageIO, pageKeyBuilder);

        for (int i = 0; i < masksCnt * BITS_IN_MASK; i++) {
            ringCache.nextFreePage();
        }

//        System.out.println("res");
//        System.out.println(masksCnt * BITS_IN_MASK);
//        System.out.println(ringCache);
        assert (((long) masksCnt ) * BITS_IN_MASK == ringCache.getSetBitsCnt());
    }

    @Test
    public void test4(@TempDir Path dir) throws IOException {
        PageBuffer myBuffer = new PageBuffer(5);
        SlottedPageIO pageIO = new SlottedPageIO(dir.toFile(), "test.db");
        PageKeyBuilder pageKeyBuilder = new PageKeyBuilder(PageType.DATA_PAGE_TYPE);
        int maxCacheElems = 10;
        BitMaskRingCache<SlottedPage> ringCache = new BitMaskRingCache<>(maxCacheElems, 10, myBuffer, pageIO, pageKeyBuilder);


        assert (1 == ringCache.isFree(2));
        ringCache.setForceNotFree(2);
        assert (0 == ringCache.isFree(2));
        ringCache.setFree(2);
        assert (1 == ringCache.isFree(2));

        assert (-1 == ringCache.isFree(2_000_000));

        long nextFreePageId = ringCache.nextFreePage();
        assert (0 == ringCache.isFree(nextFreePageId));
        ringCache.setFree(nextFreePageId);
        assert (1 == ringCache.isFree(nextFreePageId));

        assert (ringCache.tryToTakePage(4));
        assert (0 == ringCache.isFree(4));
        assert (!ringCache.tryToTakePage(4));
        ringCache.setFree(4);
        assert (1 == ringCache.isFree(4));

        assert (0 == ringCache.getSetBitsCnt());
        System.out.println(ringCache);

    }

    @Test
    public void test5(@TempDir Path dir) throws IOException {
        int rec = 64 * 1000;
        PageBuffer myBuffer = new PageBuffer(5);
        SlottedPageIO pageIO = new SlottedPageIO(dir.toFile(), "test.db");
        PageKeyBuilder pageKeyBuilder = new PageKeyBuilder(PageType.DATA_PAGE_TYPE);
        int maxCacheElems = 2;

        BitMaskRingCache<SlottedPage> ringCache = new BitMaskRingCache<>(maxCacheElems, maxCacheElems, myBuffer, pageIO, pageKeyBuilder);
        for (int i = 0; i < rec; i++) {
            long pageId = ringCache.nextFreePage();
        }

        assertEquals(rec, ringCache.getSetBitsCnt());
        assertEquals(999, ringCache.getLastMaskId());
    }

    @Test
    public void test6(@TempDir Path dir) throws IOException {
        int rec = 64 * 1000;
        PageBuffer myBuffer = new PageBuffer(5);
        SlottedPageIO pageIO = new SlottedPageIO(dir.toFile(), "test.db");
        PageKeyBuilder pageKeyBuilder = new PageKeyBuilder(PageType.DATA_PAGE_TYPE);
        int maxCacheElems = 3;

        BitMaskRingCache<SlottedPage> ringCache = new BitMaskRingCache<>(maxCacheElems, maxCacheElems, myBuffer, pageIO, pageKeyBuilder);
        for (int i = 0; i < rec; i++) {
            long pageId = ringCache.nextFreePage();
            if (i % 2 == 0) {
                ringCache.setFree(pageId);
            }
        }

        assertEquals(rec / 2, ringCache.getSetBitsCnt());
        assertEquals((rec / 2) / 64 - 1, ringCache.getLastMaskId());
    }

    @Test
    public void test44(@TempDir Path dir) throws IOException, ExecutionException, InterruptedException, TimeoutException {
        int rec = 64 * 1000;
        PageBuffer myBuffer = new PageBuffer(5);
        SlottedPageIO pageIO = new SlottedPageIO(dir.toFile(), "test.db");
        PageKeyBuilder pageKeyBuilder = new PageKeyBuilder(PageType.DATA_PAGE_TYPE);
        int maxCacheElems = 100;

        BitMaskRingCache<SlottedPage> ringCache = new BitMaskRingCache<>(maxCacheElems, maxCacheElems, myBuffer, pageIO, pageKeyBuilder);

        AtomicInteger cnt = new AtomicInteger(0);
//5807
//6806
        Function<Integer, String> func = i -> {
            try {
                int ccc = cnt.incrementAndGet();
                if (ccc % 100_000 == 0) System.out.println(ccc);
                long pageId = ringCache.nextFreePage();
                if (i > 64 * 3 - 1) {
//                    System.out.println(ringCache.cache);
                }
                assert (pageId != -1);
                if (i % 2 == 0) {
                    ringCache.setFree(pageId);
                }
            } catch (IOException e) {
                System.err.println(e);
                throw new RuntimeException(e);
            }

            return "";
        };
        Instant start = Instant.now();
        runConcurrentTest(rec, 10_000, 10, func, 200);
        Instant finish = Instant.now();
        Duration between = Duration.between(start, finish);


        System.out.println("res");
//        System.out.println(ringCache);
        System.out.println(between);
        System.out.println("Set bits:" + ringCache.getSetBitsCnt());
        System.out.println(ringCache.getFirstMaskId());
        System.out.println(ringCache.getLastMaskId());
        System.out.println(ringCache.missed.get());
        System.out.println(ringCache.getAverageMaskLoad());
    }

    public static void runConcurrentTest(int taskCount,
                                         int threadPoolAwaitTimeoutSec,
                                         int taskAwaitTimeoutSec,
                                         Function<Integer, String> function,
                                         int numberOfThreads) throws InterruptedException, ExecutionException, TimeoutException {
        ExecutorService executor = Executors.newFixedThreadPool(numberOfThreads);
        List<Future<?>> futures = new ArrayList<>(taskCount);
        for (int i = 0; i < taskCount; i++) {
            int y = i;
            Future<?> future1 = executor.submit(() -> function.apply(y));
            futures.add(future1);
        }

        executor.shutdown();
        if (!executor.awaitTermination(threadPoolAwaitTimeoutSec, TimeUnit.SECONDS)) {
            throw new InterruptedException();
        }

        for (Future<?> future : futures) {
            future.get(taskAwaitTimeoutSec, TimeUnit.SECONDS);
//            future.get();
        }

    }

}