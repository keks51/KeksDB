package com.keks.kv_storage.bplus;

import com.keks.kv_storage.bplus.buffer.CachedPage;
import com.keks.kv_storage.bplus.buffer.CachedPageNew;
import com.keks.kv_storage.bplus.buffer.PageBuffer;
import com.keks.kv_storage.bplus.page_manager.*;
import com.keks.kv_storage.bplus.page_manager.PageKey;
import com.keks.kv_storage.bplus.page_manager.page_key.PageKeyBuilder;
import com.keks.kv_storage.bplus.page_manager.page_key.PageType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;


class MyBufferTest {

    private final PageKeyBuilder pageKeyBuilder = new PageKeyBuilder(PageType.DATA_PAGE_TYPE);

    public static class PageAsHashMap extends Page {

        public final HashMap<String, String> map = new HashMap<>();

        public PageAsHashMap(long id) {
            super(id);
        }

        @Override
        public byte[] toBytes() {
            return new byte[0];
        }

        @Override
        public boolean isFull() {
            return false;
        }

    }

    public static class PageAsHashMapIO extends PageIO<PageAsHashMap> {


        private ConcurrentSkipListMap<Long, PageAsHashMap> data = new ConcurrentSkipListMap<>();

        public PageAsHashMapIO(File tableDir) throws IOException {
            super(tableDir, "test", 10);
        }

        @Override
        public PageAsHashMap getPage(long pageId) {
            if (data.get(pageId) == null) {
                return new PageAsHashMap(pageId);
            } else {
                return data.get(pageId);
            }
        }

        private final AtomicLong flushCnt = new AtomicLong();

        @Override
        public void flush(Page page) {
            long pageId = page.pageId;
            PageAsHashMap pageAsHashMap = (PageAsHashMap) page;
            try {
                Thread.sleep(1);
                printThread("Flushing: " + pageId + "  , Flush cnt: " + flushCnt.getAndAdd(1));
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            data.put(pageId, pageAsHashMap);
        }


    }


    @Test
    public void test2(@TempDir Path tmpPath) throws IOException, ExecutionException, InterruptedException, TimeoutException {
        PageAsHashMapIO pageAsHashMapManager = new PageAsHashMapIO(tmpPath.toFile());


        int bufSize = 20;
        PageBuffer myBuffer = new PageBuffer(bufSize);

        final int numberOfPages = 10;
        TableName tableName = new TableName("test1");
        for (int i = 0; i < numberOfPages; i++) {
           CachedPageNew<PageAsHashMap> andLockForWriting = myBuffer.getAndLockWrite(pageKeyBuilder.getPageKey(i), pageAsHashMapManager);
            myBuffer.unLock(andLockForWriting);
        }
//        myBuffer.flushTable(blockManagerTest);

        Random random = new Random();
        Function<Integer,CachedPageNew<PageAsHashMap>> func = (i) -> {
           CachedPageNew<PageAsHashMap> page = null;
            PageKey pageKey = pageKeyBuilder.getPageKey(random.nextInt(numberOfPages));
            try {
                String key = "key" + i;
                String value = "value" + i;
                page = myBuffer.getAndLockWrite(pageKey, pageAsHashMapManager);
                page.getPage().map.put(key, value);
                myBuffer.unLock(page);

                page = myBuffer.getAndLockRead(pageKey, pageAsHashMapManager);
                assertEquals(value, (page.getPage().map.get(key)));
                myBuffer.unLock(page);


                page = myBuffer.getAndLockWrite(pageKey, pageAsHashMapManager);
                page.getPage().map.put(key, value + "aa");
                myBuffer.unLock(page);

                page = myBuffer.getAndLockRead(pageKey, pageAsHashMapManager);
                assertEquals((value + "aa"), (page.getPage().map.get(key)));
                myBuffer.unLock(page);

                page = myBuffer.getAndLockWrite(pageKey, pageAsHashMapManager);
                page.getPage().map.remove(key);
                myBuffer.unLock(page);

                page = myBuffer.getAndLockRead(pageKey, pageAsHashMapManager);
                assertNull(page.getPage().map.get(key));
                myBuffer.unLock(page);

                return page;

            } catch (Throwable e) {
//                if (page != null) myBuffer.unLock(page);
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        };

//        System.out.println("Running test");
        runConcurrentTest2(10_0000, 300, 1000, func, 50);

//        assertEquals(10, myBuffer.getBufferCnt());
    }

    public static void runConcurrentTest2(int taskCount,
                                          int threadPoolAwaitTimeoutSec,
                                          int taskAwaitTimeoutSec,
                                          Function<Integer,CachedPageNew<PageAsHashMap>> function,
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
        }


    }

    @Test
    public void test3(@TempDir Path tmpPath) throws IOException, ExecutionException, InterruptedException, TimeoutException {
        PageAsHashMapIO pageAsHashMapManager = new PageAsHashMapIO(tmpPath.toFile());
        int bufSize = 5;
        PageBuffer myBuffer = new PageBuffer(bufSize);


        final int numberOfPages = 20;
        for (int i = 0; i < numberOfPages; i++) {
           CachedPageNew<PageAsHashMap> andLockForWriting = myBuffer.getAndLockWrite(pageKeyBuilder.getPageKey(i), pageAsHashMapManager);
            myBuffer.unLock(andLockForWriting);
        }
//        myBuffer.flushTable();

        Random random = new Random();
//        System.out.println("Running");
        Function<Integer,CachedPageNew<PageAsHashMap>> func = i -> {
           CachedPageNew<PageAsHashMap> page = null;
            PageKey pageKey = pageKeyBuilder.getPageKey(random.nextInt(numberOfPages));
            try {
                String key = "key" + i;
                String value = "value" + i;
                page = myBuffer.getAndLockWrite(pageKey, pageAsHashMapManager);
                page.getPage().map.put(key, value);
                myBuffer.unLock(page);

                page = myBuffer.getAndLockRead(pageKey, pageAsHashMapManager);
                assertEquals(value, (page.getPage().map.get(key)));
                myBuffer.unLock(page);


                page = myBuffer.getAndLockWrite(pageKey, pageAsHashMapManager);
                page.getPage().map.put(key, value + "aa");
                myBuffer.unLock(page);

                page = myBuffer.getAndLockRead(pageKey, pageAsHashMapManager);
                assertEquals((value + "aa"), (page.getPage().map.get(key)));
                myBuffer.unLock(page);

                page = myBuffer.getAndLockWrite(pageKey, pageAsHashMapManager);
                page.getPage().map.remove(key);
                myBuffer.unLock(page);

                page = myBuffer.getAndLockRead(pageKey, pageAsHashMapManager);
                assertNull(page.getPage().map.get(key));
                myBuffer.unLock(page);

                return page;
            } catch (Throwable e) {
//                System.out.println(e);
//                if (page != null) myBuffer.unLock(page);
                throw new RuntimeException(e);
            }
        };


        runConcurrentTest2(2000, 1000, 1, func, 100);

//        assertEquals(bufSize, myBuffer.getBufferCnt());
    }


}