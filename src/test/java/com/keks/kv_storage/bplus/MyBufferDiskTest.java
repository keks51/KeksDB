package com.keks.kv_storage.bplus;


import com.keks.kv_storage.bplus.buffer.CachedPageNew;
import com.keks.kv_storage.bplus.buffer.PageBuffer;
import com.keks.kv_storage.bplus.conf.BPlusConf;
import com.keks.kv_storage.bplus.item.KeyValueItem;
import com.keks.kv_storage.bplus.page_manager.managers.DataPageManager;
import com.keks.kv_storage.bplus.page_manager.page_disk.sp.SlotLocation;
import com.keks.kv_storage.bplus.page_manager.page_disk.sp.SlottedPage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;


public class MyBufferDiskTest {

//    final static Logger logger = Logger.getLogger(MyBufferDiskTest.class);

    private static final TableName tableName = new TableName("test");


    @Test
    public void test1(@TempDir Path dir) throws IOException {
        PageBuffer myBuffer = new PageBuffer(500);
        File tableDir = new File(dir.toFile(), tableName.name);
        tableDir.mkdir();
        DataPageManager dataPageManager = new DataPageManager(tableDir, myBuffer, new BPlusConf(3, 10, 10,40_000_00));

        HashMap<Long, Integer> deleteIt = new HashMap<>();
        HashMap<Integer, SlotLocation> locations = new HashMap<>();
        HashMap<Long, Boolean> freePages = new HashMap<>();

        int records = 1_000;
        {

            for (int i = 0; i < records; i++) {
               CachedPageNew<SlottedPage> cachedPage = dataPageManager.getAndLockWriteFreePage();
                long freePageId = cachedPage.pageId;
                SlottedPage page = cachedPage.getPage();
                String key = "key" + i;
                String value = "value" + i;

                short slotId = page.addItem(new KeyValueItem(key, value));
                deleteIt.put(freePageId, page.getNumberOfItems());
                locations.put(i, new SlotLocation(freePageId, slotId));

                if (page.isFull()) {
                    freePages.put(freePageId, false);
                } else {
                    freePages.put(freePageId, true);
                }
                dataPageManager.unlockPage(cachedPage);
            }

        }

//        myBuffer.flushAll();

        {
            ArrayList<Map.Entry<Long, Integer>> list = new ArrayList<>(deleteIt.entrySet());
            list.sort(Comparator.comparingLong(Map.Entry::getKey));
            for (Map.Entry<Long, Integer> e : list) {
//                System.out.println("PageId: " + e.getKey() + "   Items: " + e.getValue());
            }
            for (Map.Entry<Long, Boolean> e : freePages.entrySet()) {
//                System.out.println("PageId: " + e.getKey() + "   isSet: " + e.getValue());
            }

            for (int i = 0; i < records; i++) {
                SlotLocation location = locations.get(i);
               CachedPageNew<SlottedPage> cachedPage = dataPageManager.getAndLockReadPage(location.pageId);
                String key = "key" + i;
                String value = "value" + i;
                SlottedPage page = cachedPage.getPage();
                KeyValueItem keyValueItem = new KeyValueItem(page.getBytesReadOnly(location.slotId));
                assertEquals(key, keyValueItem.key);
                assertEquals(value, keyValueItem.value);
                assertEquals(freePages.get(location.pageId), dataPageManager.isPageFree(location.pageId) == 1);
                dataPageManager.unlockPage(cachedPage);
            }


        }

    }


    // just using buffer
    @Test
    public void test2(@TempDir Path dir) throws IOException, ExecutionException, InterruptedException, TimeoutException {
        PageBuffer myBuffer = new PageBuffer(1000);

        File tableDir = new File(dir.toFile(), tableName.name);
        tableDir.mkdir();
        DataPageManager dataPageManager = new DataPageManager(tableDir, myBuffer, new BPlusConf(3, 10, 10,40_000_00));

        ConcurrentSkipListMap<Long, Integer> deleteIt = new ConcurrentSkipListMap<>();
        ConcurrentSkipListMap<Integer, SlotLocation> locations = new ConcurrentSkipListMap<>();
        ConcurrentSkipListMap<Long, Boolean> freePages = new ConcurrentSkipListMap<>();
        int records = 100_000;
        {

            Function<Integer, String> func = i -> {
                try {
                   CachedPageNew<SlottedPage> cachedPage = dataPageManager.getAndLockWriteFreePage();
                    long freePageId = cachedPage.pageId;
                    SlottedPage page = cachedPage.getPage();
                    String key = "key" + i;
                    String value = "value" + i;

                    short slotId = page.addItem(new KeyValueItem(key, value));
                    deleteIt.put(freePageId, page.getNumberOfItems());
                    locations.put(i, new SlotLocation(freePageId, slotId));

                    if (page.isFull()) {
                        freePages.put(freePageId, false);
                    } else {
                        freePages.put(freePageId, true);
                    }
                    dataPageManager.unlockPage(cachedPage);
                    return "";
                } catch (IOException e) {
                    e.printStackTrace();
                    throw new RuntimeException("");
                }
            };
            runConcurrentTest(records, 1000, 1, func, 2);
        }



        {

            Function<Integer, String> func = i -> {
                try {
                    SlotLocation location = null;
                    SlottedPage page = null;
                    location = locations.get(i);
                   CachedPageNew<SlottedPage> cachedPage = dataPageManager.getAndLockReadPage(location.pageId);
                    String key = "key" + i;
                    String value = "value" + i;
                    page = cachedPage.getPage();
                    KeyValueItem keyValueItem = new KeyValueItem(page.getBytesReadOnly(location.slotId));
                    assertEquals(key, keyValueItem.key);
                    assertEquals(value, keyValueItem.value);
//                    assertEquals(freePages.get(location.pageId), dataPageManager.isPageFree(location.pageId) == 1);
                    dataPageManager.unlockPage(cachedPage);
                    return "";
                } catch (IOException e) {
                    e.printStackTrace();
                    throw new RuntimeException("");
                }
            };

            runConcurrentTest(records, 1000, 1, func, 200);

            ArrayList<Map.Entry<Long, Integer>> list = new ArrayList<>(deleteIt.entrySet());
            list.sort(Comparator.comparingLong(Map.Entry::getKey));
            for (Map.Entry<Long, Integer> e : list) {
//                System.out.println("PageId: " + e.getKey() + "   Items: " + e.getValue());
            }

        }


    }

    // buffer and disk
    @Test
//    @RepeatedTest(100)
    public void test3(@TempDir Path dir) throws IOException, ExecutionException, InterruptedException, TimeoutException {
        PageBuffer myBuffer = new PageBuffer(50);

        File tableDir = new File(dir.toFile(), tableName.name);
        tableDir.mkdir();
        DataPageManager dataPageManager = new DataPageManager(tableDir, myBuffer, new BPlusConf(3, 1, 1,40_000_00));

        ConcurrentSkipListMap<Long, Integer> deleteIt = new ConcurrentSkipListMap<>();
        ConcurrentSkipListMap<Integer, SlotLocation> locations = new ConcurrentSkipListMap<>();
        ConcurrentSkipListMap<Long, Boolean> freePages = new ConcurrentSkipListMap<>();
        int records = 300_000;
//        int records = 50_000;
        {

            Function<Integer, String> func = i -> {
               CachedPageNew<SlottedPage> cachedPage;
                try {
                    cachedPage = dataPageManager.getAndLockWriteFreePage();
                    long pageId = cachedPage.pageId;
                    short slotId;
                    {

                        SlottedPage page = cachedPage.getPage();
                        String key = "key" + String.format("%07d", i);;
                        String value = "value" + String.format("%07d", i);;

                        slotId = page.addItem(new KeyValueItem(key, value));
                        deleteIt.put(pageId, page.getNumberOfItems());
                        locations.put(i, new SlotLocation(pageId, slotId));
//                    printThread("Saved key " + key + " to page " + freePageId + " slot " + slotId);
                        if (page.isFull()) {
                            freePages.put(pageId, false);
                        } else {
                            freePages.put(pageId, true);
                        }
                        dataPageManager.unlockPage(cachedPage);
                    }


//                     read
                    {

                        cachedPage = dataPageManager.getAndLockReadPage(pageId);
                        String key = "key" + String.format("%07d", i);
                        String value = "value" + String.format("%07d", i);
                        SlottedPage page = cachedPage.getPage();
                        KeyValueItem keyValueItem = new KeyValueItem(page.getBytesReadOnly(slotId));
                        assertEquals(key, keyValueItem.key);
                        assertEquals(value, keyValueItem.value);
                        dataPageManager.unlockPage(cachedPage);
                    }

                    if (i % 2 == 0) {
                        // update
                        {

                            cachedPage = dataPageManager.getAndLockWritePage(pageId);
                            String key = "key" + String.format("%07d", i);
                            String value = "value";
                            SlottedPage page = cachedPage.getPage();
                            page.update(slotId, new KeyValueItem(key, value));
                            dataPageManager.unlockPage(cachedPage);
                        }

                        // read
                        {

                            cachedPage = dataPageManager.getAndLockReadPage(pageId);
                            String key = "key" + String.format("%07d", i);
                            String value = "value";
                            SlottedPage page = cachedPage.getPage();
                            KeyValueItem keyValueItem = new KeyValueItem(page.getBytesReadOnly(slotId));
                            assertEquals(key, keyValueItem.key);
                            assertEquals(value, keyValueItem.value);
                            dataPageManager.unlockPage(cachedPage);
                        }
                    }

                    if (i % 3 == 0) {
                        // delete
                        {
                            ;
                            try {
                                cachedPage = dataPageManager.getAndLockWritePage(pageId);
                                SlottedPage page = cachedPage.getPage();
                                boolean wasFull = page.isFull();
                                page.delete(slotId);
//                            System.out.println("Deleting Page: " + pageId + " P: " + page.pageId + "  Slot: " + slotId);
//                                if (wasFull && !page.isFull() && cachedPage.getInUseCnt() == 1) {
//                                    cachedPage.lock();
//                                    if (!page.isFull() && cachedPage.getInUseCnt() == 1) {
//
//                                        freePages.put(pageId, true);
//
////                                        printThread("Deleting CCC PageId: " + pageId + " SlotId: " + slotId);
////                                        pagesManager.setDataPageAsFree(pageId);
////                                        printThread("Deleted CCC PageId: " + pageId + " SlotId: " + slotId);
//                                    }
//
//
//                                }

                                dataPageManager.unlockPage(cachedPage);
                            } finally {
//                                spaceLock.writeLock().unlock();
//                                printThread("Return space lock");
                            }

                        }
                    }

                    return "";
                } catch (Throwable e) {
                    e.printStackTrace();
                    System.exit(1);
                    throw new RuntimeException("");
                }
            };
            runConcurrentTest(records, 1000, 1, func, 1);
            ArrayList<Map.Entry<Long, Integer>> list = new ArrayList<>(deleteIt.entrySet());
            list.sort(Comparator.comparingLong(Map.Entry::getKey));
            for (Map.Entry<Long, Integer> e : list) {
//                System.out.println("PageId: " + e.getKey() + "   Items: " + e.getValue());
            }
        }

        ArrayList<Map.Entry<Long, Integer>> list = new ArrayList<>(deleteIt.entrySet());
        list.sort(Comparator.comparingLong(Map.Entry::getKey));
        for (Map.Entry<Long, Integer> e : list) {
//            System.out.println("PageId: " + e.getKey() + "   Items: " + e.getValue());
        }


    }

    // buffer and disk
    // direct 3min 22sec
    // without_direct dsync 2min 20sec
    // without_direct sync 2min 45sec
    // unplugged notebook
    // sync 4min32sec
    // sync with direct too long
//    @Test
//    @RepeatedTest(10)
    @Test
//    @RepeatedTest(10)
    public void test4(@TempDir Path dir) throws IOException, ExecutionException, InterruptedException, TimeoutException {
        PageBuffer myBuffer = new PageBuffer(50_000);

        File tableDir = new File(dir.toFile(), tableName.name);
        tableDir.mkdir();
        DataPageManager dataPageManager = new DataPageManager(tableDir, myBuffer, new BPlusConf(3, 10, 10,40_000_00));

        ConcurrentSkipListMap<Long, Integer> deleteIt = new ConcurrentSkipListMap<>();
        ConcurrentSkipListMap<Integer, SlotLocation> locations = new ConcurrentSkipListMap<>();
        ConcurrentSkipListMap<Long, Boolean> freePages = new ConcurrentSkipListMap<>();
        int records = 1_000_000;
//        int records = 100_000;
        AtomicInteger deletedCnt = new AtomicInteger(0);
        AtomicInteger cnt = new AtomicInteger(0);
        {
            Function<Integer, String> func = i -> {


               CachedPageNew<SlottedPage> cachedPage = null;
                try {
                    cachedPage = dataPageManager.getAndLockWriteFreePage();
                    long pageId = cachedPage.pageId;
                    short slotId;
                    // add
                    {
                        SlottedPage page = cachedPage.getPage();
                        String key = "key" + String.format("%07d", i);
                        String value = "value" + String.format("%07d", i);
                        try {
                            slotId = page.addItem(new KeyValueItem(key, value));
//                            printThread("add to page " + page.pageId + " slot " + slotId + "NumberOfSlots: "
//                                    + cachedPage.getPage().getNextSlotId() +  " PageInUse: " + cachedPage.getInUseCnt());
                        } catch (RuntimeException e) {
//                            printThread("Added CCC PageId: " + pageId + " SlotId: " + slotId + " Key: " + key + " Value: " + value);
                            throw e;
                        }

                        deleteIt.put(pageId, page.getNumberOfItems());
                        locations.put(i, new SlotLocation(pageId, slotId));

                        if (page.isFull()) {
                            freePages.put(pageId, false);
                        } else {
                            freePages.put(pageId, true);
                        }

                        dataPageManager.unlockPage(cachedPage);

                    }

//                     read
                    {

                        cachedPage = dataPageManager.getAndLockReadPage(pageId);
                        String key = "key" + String.format("%07d", i);
                        String value = "value" + String.format("%07d", i);
                        SlottedPage page = cachedPage.getPage();
                        KeyValueItem keyValueItem = new KeyValueItem(page.getBytesReadOnly(slotId));
                        assertEquals(key, keyValueItem.key);
                        assertEquals(value, keyValueItem.value);
                        dataPageManager.unlockPage(cachedPage);
                    }

                    if (i % 2 == 0) {
                        // update
                        {

                            cachedPage = dataPageManager.getAndLockWritePage(pageId);
                            String key = "key" + String.format("%07d", i);
                            String value = "value";
                            SlottedPage page = cachedPage.getPage();
                            page.update(slotId, new KeyValueItem(key, value));
                            dataPageManager.unlockPage(cachedPage);
                        }

                        // read
                        {

                            cachedPage = dataPageManager.getAndLockReadPage(pageId);
                            String key = "key" + String.format("%07d", i);
                            String value = "value";
                            SlottedPage page = cachedPage.getPage();
                            KeyValueItem keyValueItem = new KeyValueItem(page.getBytesReadOnly(slotId));
                            assertEquals(key, keyValueItem.key);
                            assertEquals(value, keyValueItem.value);
                            dataPageManager.unlockPage(cachedPage);
                        }
                    }

                    if (i % 3 == 0) {
                        // delete
                        {
                            ;
                            try {
                                cachedPage = dataPageManager.getAndLockWritePage(pageId);
                                SlottedPage page = cachedPage.getPage();
                                boolean wasFull = page.isFull();
                                page.delete(slotId);
//                            System.out.println("Deleting Page: " + pageId + " P: " + page.pageId + "  Slot: " + slotId);
//                                if (wasFull && !page.isFull() && cachedPage.getInUseCnt() == 1) {
//                                    cachedPage.lock();
//                                    if (!page.isFull() && cachedPage.getInUseCnt() == 1) {
//                                        deletedCnt.incrementAndGet();
//                                        freePages.put(pageId, true);
//
////                                        printThread("Deleting CCC PageId: " + pageId + " SlotId: " + slotId);
////                                        pagesManager.setDataPageAsFree(pageId);
////                                        printThread("Deleted CCC PageId: " + pageId + " SlotId: " + slotId);
//                                    }
//                                    cachedPage.unlock();
//
//                                }

                                dataPageManager.unlockPage(cachedPage);
                            } finally {
//                                spaceLock.writeLock().unlock();
//                                printThread("Return space lock");
                            }

                        }

                        // read
//                        {
//
//                            try {
//                                cachedPage = dataPageManager.getAndLockReadPage(pageId);
//                                SlottedPage page = (SlottedPage) cachedPage.getPage();
////                                if (page.doesSlotExist(slotId)) {
////                                    if (!page.isSlotDeleted(slotId)) {
////                                        System.out.println("hahahaha" + slotId);
////                                        System.out.println(page.dump());
////                                    }
////                                    assertTrue(page.isSlotDeleted(slotId));
////
////                                } else {
////
////                                }
//
//                            } finally {
//                                dataPageManager.unlockPage(cachedPage);
//                            }
//
//                        }
                    }

                    return "";
                } catch (Throwable e) {
                    e.printStackTrace();
                    System.err.println("Error thread " + Thread.currentThread().getId());
                    System.err.println(cachedPage.sb);
                    System.exit(1);
                    e.printStackTrace();
                    throw new RuntimeException(e);
                } finally {
                    int ccc = cnt.incrementAndGet();
                    if (ccc % 10_000 == 0) System.err.println(ccc);
                }
            };
            runConcurrentTest(records, 1000, 1, func, 50);
            System.out.println("printing pages capacity");
//            System.out.println("Read: " + fromDiskPagesReader.dataPageIO.read.get());
//            System.out.println("Write: " + fromDiskPagesReader.dataPageIO.write.get());
//            ArrayList<Map.Entry<Long, Integer>> list = new ArrayList<>(deleteIt.entrySet());
//            list.sort(Comparator.comparingLong(Map.Entry::getKey));
//            for (Map.Entry<Long, Integer> e : list) {
//                System.out.println("PageId: " + e.getKey() + "   Items: " + e.getValue() + " Is free:" + dataPageManager.freeSpaceChecker.isFree(e.getKey()));
//            }
//            System.out.println("Pages " + list.size());
//            System.out.println("DeletedCnt: " + deletedCnt.get());
        }


//        {
//            Function<Integer, String> func = i -> {
//                if (i % 3 != 0) {
////                    printThread("" + i);
//                    try {
//                        SlotLocation location = locations.get(i);
//                       CachedPageNew<SlottedPage> cachedPage = dataPageManager.getAndLockReadPage(location.pageId);
//                        String key = "key" + String.format("%07d", i);
//                        String value = "value" + String.format("%07d", i);
//                        if (i % 2 == 0) {
//                            value = "value";
//                        }
//                        SlottedPage page = cachedPage.getPage();
//                        KeyValueItem keyValueItem = new KeyValueItem(page.getBytesReadOnly(location.slotId));
//
//                        dataPageManager.unlockPage(cachedPage);
//                        assertEquals(key, keyValueItem.key);
//                        assertEquals(value, keyValueItem.value);
////                        assertEquals(freePages.get(location.pageId), freeSpaceChecker.isFree(location.pageId) == 0);
//                        return "";
//                    } catch (IOException e) {
//
//                        e.printStackTrace();
//                        throw new RuntimeException("");
//                    } finally {
//
//                    }
//                }
//                return "";
//            };
//
//            runConcurrentTest(records, 1000, 1, func, 200);
//        }


    }


    // buffer and disk
    @Test
    public void test5(@TempDir Path dir) throws IOException, ExecutionException, InterruptedException, TimeoutException {
        PageBuffer myBuffer = new PageBuffer(100_000);

        File tableDir = new File(dir.toFile(), tableName.name);
        tableDir.mkdir();
        DataPageManager dataPageManager = new DataPageManager(tableDir, myBuffer, new BPlusConf(3, 10, 10,40_000_00));

        ConcurrentSkipListMap<Long, Integer> deleteIt = new ConcurrentSkipListMap<>();
        ConcurrentSkipListMap<Integer, SlotLocation> locations = new ConcurrentSkipListMap<>();
        ConcurrentSkipListMap<Long, Boolean> freePages = new ConcurrentSkipListMap<>();
        int records = 100_000;
        AtomicInteger atomicInteger = new AtomicInteger(0);
        AtomicInteger deletedCnt = new AtomicInteger(0);
        {
            Function<Integer, String> func = i -> {
               CachedPageNew<SlottedPage> cachedPage = null;

                try {
                    cachedPage = dataPageManager.getAndLockWriteFreePage();
                    long pageId = cachedPage.pageId;
                    short slotId = 0;
                    // add
                    {
                        SlottedPage page = cachedPage.getPage();
                        String key = "key" + i;
                        String value = "value" + i;
                        try {
                            slotId = page.addItem(new KeyValueItem(key, value));
                        } catch (RuntimeException e) {
                            printThread("Added CCC PageId: " + pageId + " SlotId: " + slotId + " Key: " + key + " Value: " + value);
                            throw e;
                        }

                        deleteIt.put(pageId, page.getNumberOfItems());
                        locations.put(i, new SlotLocation(pageId, slotId));

                        if (page.isFull()) {
                            freePages.put(pageId, false);
                        } else {
                            freePages.put(pageId, true);
                        }
                        dataPageManager.unlockPage(cachedPage);
                    }

//                     read
                    {

                        cachedPage = dataPageManager.getAndLockReadPage(pageId);
                        String key = "key" + i;
                        String value = "value" + i;
                        SlottedPage page = cachedPage.getPage();
                        KeyValueItem keyValueItem = new KeyValueItem(page.getBytesReadOnly(slotId));
                        assertEquals(key, keyValueItem.key);
                        assertEquals(value, keyValueItem.value);
                        dataPageManager.unlockPage(cachedPage);
                    }


                    if (i % 2 == 0) {
                        // update
                        {

                            cachedPage = dataPageManager.getAndLockWritePage(pageId);
                            String key = "key" + i;
                            String value = "value";
                            SlottedPage page = cachedPage.getPage();
                            page.update(slotId, new KeyValueItem(key, value));
                            dataPageManager.unlockPage(cachedPage);
                        }

                        // read
                        {

                            cachedPage = dataPageManager.getAndLockReadPage(pageId);
                            String key = "key" + i;
                            String value = "value";
                            SlottedPage page = cachedPage.getPage();
                            KeyValueItem keyValueItem = new KeyValueItem(page.getBytesReadOnly(slotId));
                            assertEquals(key, keyValueItem.key);
                            assertEquals(value, keyValueItem.value);
                            dataPageManager.unlockPage(cachedPage);
                        }
                    }

                    if (i % 3 == 0) {
                        // delete
                        {
                            ;
                            cachedPage = dataPageManager.getAndLockWritePage(pageId);
                            SlottedPage page = (SlottedPage) cachedPage.getPage();
                            boolean wasFull = page.isFull();
                            page.delete(slotId);
//                            System.out.println("Deleting Page: " + pageId + " P: " + page.pageId + "  Slot: " + slotId);
//                            if (wasFull && !page.isFull() && cachedPage.getInUseCnt() == 1) {
//                                cachedPage.lock();
//                                if (!page.isFull() && cachedPage.getInUseCnt() == 1) {
//                                    deletedCnt.incrementAndGet();
//                                    freePages.put(pageId, true);
//
////                                        printThread("Deleting CCC PageId: " + pageId + " SlotId: " + slotId);
////                                        pagesManager.setDataPageAsFree(pageId);
////                                        printThread("Deleted CCC PageId: " + pageId + " SlotId: " + slotId);
//                                }
//                                cachedPage.unlock();
//
//                            }

                            dataPageManager.unlockPage(cachedPage);

                        }

                        // read
                        {

                            try {
                                cachedPage = dataPageManager.getAndLockReadPage(pageId);
                                SlottedPage page = cachedPage.getPage();
                            } finally {
                                dataPageManager.unlockPage(cachedPage);
                            }

                        }
                    }
//                    printThread("Finished " + i);
                    return "";
                } catch (Throwable e) {
                    e.printStackTrace();
                    throw new RuntimeException(e);
                }
            };
            runConcurrentTest(records, 1000, 1, func, 200);
//            System.out.println("Read: " + fromDiskPagesReader.dataPageIO.read.get());
//            System.out.println("Write: " + fromDiskPagesReader.dataPageIO.write.get());
            ArrayList<Map.Entry<Long, Integer>> list = new ArrayList<>(deleteIt.entrySet());
            list.sort(Comparator.comparingLong(Map.Entry::getKey));
//            for (Map.Entry<Long, Integer> e : list) {
//                System.out.println("PageId: " + e.getKey() + "   Items: " + e.getValue());
//            }

        }


        {

            Function<Integer, String> func = i -> {
                if (i % 3 != 0) {
//                    printThread("" + i);
                    try {
                        SlotLocation location = locations.get(i);
                       CachedPageNew<SlottedPage> cachedPage = dataPageManager.getAndLockReadPage(location.pageId);
                        String key = "key" + i;
                        String value = "value" + i;
                        if (i % 2 == 0) {
                            value = "value";
                        }
                        SlottedPage page = cachedPage.getPage();
                        KeyValueItem keyValueItem = new KeyValueItem(page.getBytesReadOnly(location.slotId));

                        dataPageManager.unlockPage(cachedPage);
                        assertEquals(key, keyValueItem.key);
                        assertEquals(value, keyValueItem.value);
//                        assertEquals(freePages.get(location.pageId), freeSpaceChecker.isFree(location.pageId) == 0);
                        return "";
                    } catch (IOException e) {

                        e.printStackTrace();
                        throw new RuntimeException("");
                    }
                }
                return "";
            };

            runConcurrentTest(records, 1000, 1, func, 200);
        }

        ArrayList<Map.Entry<Long, Integer>> list = new ArrayList<>(deleteIt.entrySet());
        list.sort(Comparator.comparingLong(Map.Entry::getKey));
        for (Map.Entry<Long, Integer> e : list) {
//            System.out.println("PageId: " + e.getKey() + "   Items: " + e.getValue());
        }
//        System.out.println("Pages " + list.size());
        dataPageManager.printStatistics();

    }


    //Read: 8331
    //Read: 8279

    //Read: 1521
    //Read: 1353
    //PageId: 233   Items: 1
    //PageId: 234   Items: 1

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


    private void printThread(String msg) {
//        System.out.println("Thread[" + Thread.currentThread().getId() + "]  " + msg);
//        logger.debug("Thread[" + Thread.currentThread().getId() + "]: " + msg);
    }


    @Test
//    @RepeatedTest(1000)
    public void test10(@TempDir Path dir) throws IOException, ExecutionException, InterruptedException, TimeoutException {
        PageBuffer myBuffer = new PageBuffer(50);

        File tableDir = new File(dir.toFile(), tableName.name);
        tableDir.mkdir();
        DataPageManager dataPageManager = new DataPageManager(tableDir, myBuffer, new BPlusConf(3, 10, 10,40_000_00));

        ConcurrentSkipListMap<Long, Integer> deleteIt = new ConcurrentSkipListMap<>();
        ConcurrentSkipListMap<Integer, SlotLocation> locations = new ConcurrentSkipListMap<>();
        ConcurrentSkipListMap<Long, Boolean> freePages = new ConcurrentSkipListMap<>();
        int records = 1_000_000;
//        int records = 100_000;
        AtomicInteger deletedCnt = new AtomicInteger(0);
        AtomicInteger cnt = new AtomicInteger(0);
        {
            Function<Integer, String> func = i -> {


               CachedPageNew<SlottedPage> cachedPage = null;
                try {
                    cachedPage = dataPageManager.getAndLockWriteFreePage();
                    long pageId = cachedPage.pageId;
                    short slotId = 0;
                    // add
                    {
                        SlottedPage page = cachedPage.getPage();
                        String key = "key" + String.format("%07d", i);
                        String value = "value" + String.format("%07d", i) + "00";
                        try {
                            slotId = page.addItem(new KeyValueItem(key, value));
//                            printThread("add to page " + page.pageId + " slot " + slotId + "NumberOfSlots: "
//                                    + cachedPage.getPage().getNextSlotId() + " PageInUse: " + cachedPage.getInUseCnt()
//                                    + " Key: " + key + " Value: " + value);
                        } catch (RuntimeException e) {
//                            printThread("Added CCC PageId: " + pageId + " SlotId: " + slotId + " Key: " + key + " Value: " + value);
                            throw e;
                        }

                        deleteIt.put(pageId, page.getNumberOfItems());
                        locations.put(i, new SlotLocation(pageId, slotId));

                        if (page.isFull()) {
                            freePages.put(pageId, false);
                        } else {
                            freePages.put(pageId, true);
                        }

                        dataPageManager.unlockPage(cachedPage);

                    }

//                     read
                    {

                        cachedPage = dataPageManager.getAndLockReadPage(pageId);
                        String key = "key" + String.format("%07d", i);
                        String value = "value" + String.format("%07d", i) + "00";
                        SlottedPage page = cachedPage.getPage();
                        KeyValueItem keyValueItem = new KeyValueItem(page.getBytesReadOnly(slotId));
                        assertEquals(key, keyValueItem.key, "PageId: " + pageId + " SlotId: " + slotId);
                        assertEquals(value, keyValueItem.value, "PageId: " + pageId + " SlotId: " + slotId);
                        dataPageManager.unlockPage(cachedPage);
                    }

                    {
                        {
                            if (i % 23 == 0) {
                                // update
                                {

                                    cachedPage = dataPageManager.getAndLockWritePage(pageId);
                                    String key = "key" + String.format("%07d", i);
                                    String value = "value01" + String.format("%07d", i);
                                    SlottedPage page = cachedPage.getPage();
                                    page.update(slotId, new KeyValueItem(key, value));
//                                    printThread("update page " + page.pageId + " slot " + slotId + "NumberOfSlots: "
//                                            + cachedPage.getPage().getNextSlotId() + " PageInUse: " + cachedPage.getInUseCnt()
//                                            + " Key: " + key + " Value: " + value);
                                    dataPageManager.unlockPage(cachedPage);
                                }

                                // read
                                {

                                    cachedPage = dataPageManager.getAndLockReadPage(pageId);
                                    String key = "key" + String.format("%07d", i);
                                    String value = "value01" + String.format("%07d", i);
                                    SlottedPage page = cachedPage.getPage();
                                    KeyValueItem keyValueItem = new KeyValueItem(page.getBytesReadOnly(slotId));
                                    assertEquals(key, keyValueItem.key, "PageId: " + pageId + " SlotId: " + slotId);
                                    assertEquals(value, keyValueItem.value, "PageId: " + pageId + " SlotId: " + slotId);
                                    dataPageManager.unlockPage(cachedPage);
                                }
                            }
                        }

                        {
                            if (i % 19 == 0) {
                                // update
                                {

                                    cachedPage = dataPageManager.getAndLockWritePage(pageId);
                                    String key = "key" + String.format("%07d", i);
                                    String value = "value02" + String.format("%07d", i);
                                    SlottedPage page = cachedPage.getPage();
                                    page.update(slotId, new KeyValueItem(key, value));
                                    printThread("update page " + page.pageId + " slot " + slotId + "NumberOfSlots: "
                                            + cachedPage.getPage().getNextSlotId() + " PageInUse: " + cachedPage.getInUseCnt()
                                            + " Key: " + key + " Value: " + value);
                                    dataPageManager.unlockPage(cachedPage);
                                }

                                // read
                                {

                                    cachedPage = dataPageManager.getAndLockReadPage(pageId);
                                    String key = "key" + String.format("%07d", i);
                                    String value = "value02" + String.format("%07d", i);
                                    SlottedPage page = cachedPage.getPage();
                                    KeyValueItem keyValueItem = new KeyValueItem(page.getBytesReadOnly(slotId));
                                    assertEquals(key, keyValueItem.key, "PageId: " + pageId + " SlotId: " + slotId);
                                    assertEquals(value, keyValueItem.value, "PageId: " + pageId + " SlotId: " + slotId);
                                    dataPageManager.unlockPage(cachedPage);
                                }
                            }
                        }

                        {
                            if (i % 17 == 0) {
                                // update
                                {

                                    cachedPage = dataPageManager.getAndLockWritePage(pageId);
                                    String key = "key" + String.format("%07d", i);
                                    String value = "value03" + String.format("%07d", i);
                                    SlottedPage page = cachedPage.getPage();
                                    page.update(slotId, new KeyValueItem(key, value));
//                                    printThread("update page " + page.pageId + " slot " + slotId + "NumberOfSlots: "
//                                            + cachedPage.getPage().getNextSlotId() + " PageInUse: " + cachedPage.getInUseCnt()
//                                            + " Key: " + key + " Value: " + value);
                                    dataPageManager.unlockPage(cachedPage);
                                }

                                // read
                                {

                                    cachedPage = dataPageManager.getAndLockReadPage(pageId);
                                    String key = "key" + String.format("%07d", i);
                                    String value = "value03" + String.format("%07d", i);
                                    SlottedPage page = cachedPage.getPage();
                                    KeyValueItem keyValueItem = new KeyValueItem(page.getBytesReadOnly(slotId));
                                    assertEquals(key, keyValueItem.key, "PageId: " + pageId + " SlotId: " + slotId);
                                    assertEquals(value, keyValueItem.value, "PageId: " + pageId + " SlotId: " + slotId);
                                    dataPageManager.unlockPage(cachedPage);
                                }
                            }
                        }

                        {
                            if (i % 13 == 0) {
                                // update
                                {
                                    cachedPage = dataPageManager.getAndLockWritePage(pageId);
                                    String key = "key" + String.format("%07d", i);
                                    String value = "value04" + String.format("%07d", i);
                                    SlottedPage page = cachedPage.getPage();
                                    page.update(slotId, new KeyValueItem(key, value));
                                    printThread("update page " + page.pageId + " slot " + slotId + "NumberOfSlots: "
                                            + cachedPage.getPage().getNextSlotId() + " PageInUse: " + cachedPage.getInUseCnt()
                                            + " Key: " + key + " Value: " + value);
                                    dataPageManager.unlockPage(cachedPage);
                                }

                                // read
                                {

                                    cachedPage = dataPageManager.getAndLockReadPage(pageId);
                                    String key = "key" + String.format("%07d", i);
                                    String value = "value04" + String.format("%07d", i);
                                    SlottedPage page = cachedPage.getPage();
                                    KeyValueItem keyValueItem = new KeyValueItem(page.getBytesReadOnly(slotId));
                                    assertEquals(key, keyValueItem.key, "PageId: " + pageId + " SlotId: " + slotId);
                                    assertEquals(value, keyValueItem.value, "PageId: " + pageId + " SlotId: " + slotId);
                                    dataPageManager.unlockPage(cachedPage);
                                }
                            }
                        }

                        {
                            if (i % 11 == 0) {
                                // update
                                {

                                    cachedPage = dataPageManager.getAndLockWritePage(pageId);
                                    String key = "key" + String.format("%07d", i);
                                    String value = "value05" + String.format("%07d", i);
                                    SlottedPage page = cachedPage.getPage();
                                    page.update(slotId, new KeyValueItem(key, value));
                                    printThread("update page " + page.pageId + " slot " + slotId + "NumberOfSlots: "
                                            + cachedPage.getPage().getNextSlotId() + " PageInUse: " + cachedPage.getInUseCnt()
                                            + " Key: " + key + " Value: " + value);
                                    dataPageManager.unlockPage(cachedPage);
                                }

                                // read
                                {

                                    cachedPage = dataPageManager.getAndLockReadPage(pageId);
                                    String key = "key" + String.format("%07d", i);
                                    String value = "value05" + String.format("%07d", i);
                                    SlottedPage page = cachedPage.getPage();
                                    KeyValueItem keyValueItem = new KeyValueItem(page.getBytesReadOnly(slotId));
                                    assertEquals(key, keyValueItem.key, "PageId: " + pageId + " SlotId: " + slotId);
                                    assertEquals(value, keyValueItem.value, "PageId: " + pageId + " SlotId: " + slotId);
                                    dataPageManager.unlockPage(cachedPage);
                                }
                            }
                        }

                        {
                            if (i % 7 == 0) {
                                // update
                                {

                                    cachedPage = dataPageManager.getAndLockWritePage(pageId);
                                    String key = "key" + String.format("%07d", i);
                                    String value = "value06" + String.format("%07d", i);
                                    SlottedPage page = cachedPage.getPage();
                                    page.update(slotId, new KeyValueItem(key, value));
                                    printThread("update page " + page.pageId + " slot " + slotId + "NumberOfSlots: "
                                            + cachedPage.getPage().getNextSlotId() + " PageInUse: " + cachedPage.getInUseCnt()
                                            + " Key: " + key + " Value: " + value);
                                    dataPageManager.unlockPage(cachedPage);
                                }

                                // read
                                {

                                    cachedPage = dataPageManager.getAndLockReadPage(pageId);
                                    String key = "key" + String.format("%07d", i);
                                    String value = "value06" + String.format("%07d", i);
                                    SlottedPage page = cachedPage.getPage();
                                    KeyValueItem keyValueItem = new KeyValueItem(page.getBytesReadOnly(slotId));
                                    assertEquals(key, keyValueItem.key, "PageId: " + pageId + " SlotId: " + slotId);
                                    assertEquals(value, keyValueItem.value, "PageId: " + pageId + " SlotId: " + slotId);
                                    dataPageManager.unlockPage(cachedPage);
                                }
                            }
                        }

                        {
                            if (i % 5 == 0) {
                                // update
                                {

                                    cachedPage = dataPageManager.getAndLockWritePage(pageId);
                                    String key = "key" + String.format("%07d", i);
                                    String value = "value07" + String.format("%07d", i);
                                    SlottedPage page = cachedPage.getPage();
                                    page.update(slotId, new KeyValueItem(key, value));
                                    printThread("update page " + page.pageId + " slot " + slotId + "NumberOfSlots: "
                                            + cachedPage.getPage().getNextSlotId() + " PageInUse: " + cachedPage.getInUseCnt()
                                            + " Key: " + key + " Value: " + value);
                                    dataPageManager.unlockPage(cachedPage);
                                }

                                // read
                                {

                                    cachedPage = dataPageManager.getAndLockReadPage(pageId);
                                    String key = "key" + String.format("%07d", i);
                                    String value = "value07" + String.format("%07d", i);
                                    SlottedPage page = cachedPage.getPage();
                                    KeyValueItem keyValueItem = new KeyValueItem(page.getBytesReadOnly(slotId));
                                    assertEquals(key, keyValueItem.key, "PageId: " + pageId + " SlotId: " + slotId);
                                    assertEquals(value, keyValueItem.value, "PageId: " + pageId + " SlotId: " + slotId);
                                    dataPageManager.unlockPage(cachedPage);
                                }
                            }
                        }

                        {
                            if (i % 3 == 0) {
                                // update
                                {

                                    cachedPage = dataPageManager.getAndLockWritePage(pageId);
                                    String key = "key" + String.format("%07d", i);
                                    String value = "value08" + String.format("%07d", i);
                                    SlottedPage page = cachedPage.getPage();
                                    page.update(slotId, new KeyValueItem(key, value));
                                    printThread("update page " + page.pageId + " slot " + slotId + "NumberOfSlots: "
                                            + cachedPage.getPage().getNextSlotId() + " PageInUse: " + cachedPage.getInUseCnt()
                                            + " Key: " + key + " Value: " + value);
                                    dataPageManager.unlockPage(cachedPage);
                                }

                                // read
                                {

                                    cachedPage = dataPageManager.getAndLockReadPage(pageId);
                                    String key = "key" + String.format("%07d", i);
                                    String value = "value08" + String.format("%07d", i);
                                    SlottedPage page = cachedPage.getPage();
                                    KeyValueItem keyValueItem = new KeyValueItem(page.getBytesReadOnly(slotId));
                                    assertEquals(key, keyValueItem.key, "PageId: " + pageId + " SlotId: " + slotId);
                                    assertEquals(value, keyValueItem.value, "PageId: " + pageId + " SlotId: " + slotId);
                                    dataPageManager.unlockPage(cachedPage);
                                }
                            }
                        }

                        {
                            if (i % 2 == 0) {
                                // update
                                {

                                    cachedPage = dataPageManager.getAndLockWritePage(pageId);
                                    String key = "key" + String.format("%07d", i);
                                    String value = "value09" + String.format("%07d", i);
                                    SlottedPage page = cachedPage.getPage();
                                    page.update(slotId, new KeyValueItem(key, value));
                                    printThread("update page " + page.pageId + " slot " + slotId + "NumberOfSlots: "
                                            + cachedPage.getPage().getNextSlotId() + " PageInUse: " + cachedPage.getInUseCnt()
                                            + " Key: " + key + " Value: " + value);
                                    dataPageManager.unlockPage(cachedPage);
                                }

                                // read
                                {

                                    cachedPage = dataPageManager.getAndLockReadPage(pageId);
                                    String key = "key" + String.format("%07d", i);
                                    String value = "value09" + String.format("%07d", i);
                                    SlottedPage page = cachedPage.getPage();
                                    KeyValueItem keyValueItem = new KeyValueItem(page.getBytesReadOnly(slotId));
                                    assertEquals(key, keyValueItem.key, "PageId: " + pageId + " SlotId: " + slotId);
                                    assertEquals(value, keyValueItem.value, "PageId: " + pageId + " SlotId: " + slotId);
                                    dataPageManager.unlockPage(cachedPage);
                                }
                            }

                        }

//                    {
//                        if (i % 10 == 0) {
//                            // update
//                            {
//
//                                cachedPage = dataPageManager.getAndLockWritePage(pageId);
//                                String key = "key" + String.format("%07d", i);
//                                String value = "value10" + String.format("%07d", i);
//                                SlottedPage page = cachedPage.getPage();
//                                page.update(slotId, new KeyValueItem(key, value));
//                                dataPageManager.unlockPage(cachedPage);
//                            }
//
//                            // read
//                            {
//
//                                cachedPage = dataPageManager.getAndLockReadPage(pageId);
//                                String key = "key" + String.format("%07d", i);
//                                String value = "value10" + String.format("%07d", i);
//                                SlottedPage page = cachedPage.getPage();
//                                KeyValueItem keyValueItem = new KeyValueItem(page.getBytesReadOnly(slotId));
//                                assertEquals(key, keyValueItem.key, "PageId: " + pageId + " SlotId: " + slotId);
//                                assertEquals(value, keyValueItem.value, "PageId: " + pageId + " SlotId: " + slotId);
//                                dataPageManager.unlockPage(cachedPage);
//                            }
//                        }
//                    }
                    }


                    return "";
                } catch (Throwable e) {
                    e.printStackTrace();
                    System.err.println("Error thread " + Thread.currentThread().getId());
                    System.err.println("Buff Elems size" + myBuffer.lruCache.size());
                    System.err.println(cachedPage.sb.toString());
                    e.printStackTrace();
                    System.exit(1);

                    throw new RuntimeException(e);
                } finally {
                    int ccc = cnt.incrementAndGet();
                    if (ccc % 10_000 == 0) System.err.println(ccc);
                }
            };
            runConcurrentTest(records, 1000, 1, func, 2000);

//            System.out.println("Read: " + fromDiskPagesReader.dataPageIO.read.get());
//            System.out.println("Write: " + fromDiskPagesReader.dataPageIO.write.get());
//            ArrayList<Map.Entry<Long, Integer>> list = new ArrayList<>(deleteIt.entrySet());
//            list.sort(Comparator.comparingLong(Map.Entry::getKey));
//            for (Map.Entry<Long, Integer> e : list) {
//                System.out.println("PageId: " + e.getKey() + "   Items: " + e.getValue() + " Is free:" + dataPageManager.freeSpaceChecker.isFree(e.getKey()));
//            }
//            System.out.println("DeletedCnt: " + deletedCnt.get());


        }

//        System.out.println("Reading all");
//        {
//            Function<Integer, String> func = i -> {
//                if (i % 3 != 0) {
////                    printThread("" + i);
//                    try {
//                        SlotLocation location = locations.get(i);
//                       CachedPageNew<SlottedPage> cachedPage = dataPageManager.getAndLockReadPage(location.pageId);
//                        String key = "key" + String.format("%07d", i);
//                        String value = "value" + String.format("%07d", i);
//                        if (i % 2 == 0) {
//                            value = "value";
//                        }
//                        SlottedPage page = cachedPage.getPage();
//                        KeyValueItem keyValueItem = new KeyValueItem(page.getBytesReadOnly(location.slotId));
//
//                        dataPageManager.unlockPage(cachedPage);
//                        assertEquals(key, keyValueItem.key, "PageId: " + pageId + " SlotId: " + slotId);
//                        assertEquals(value, keyValueItem.value, "PageId: " + pageId + " SlotId: " + slotId);
////                        assertEquals(freePages.get(location.pageId), freeSpaceChecker.isFree(location.pageId) == 0);
//                        return "";
//                    } catch (IOException e) {
//
//                        e.printStackTrace();
//                        throw new RuntimeException("");
//                    } finally {
//
//                    }
//                }
//                return "";
//            };
//
//            runConcurrentTest(records, 1000, 1, func, 200);
//        }
////
//        ArrayList<Map.Entry<Long, Integer>> list = new ArrayList<>(deleteIt.entrySet());
//        list.sort(Comparator.comparingLong(Map.Entry::getKey));
//        for (Map.Entry<Long, Integer> e : list) {
//            System.out.println("PageId: " + e.getKey() + "   Items: " + e.getValue());
//        }
//        System.out.println("Pages " + list.size());

    }

    // buffer and disk
//    @Test
//    @RepeatedTest(100)
//    public void test31(@TempDir Path dir) throws IOException, ExecutionException, InterruptedException, TimeoutException {
//        PageBuffer myBuffer = new PageBuffer(100);
//        int numberOfThreads = 2000;
//        File tableDir = new File(dir.toFile(), tableName.name);
//        tableDir.mkdir();
//        DataPageManager dataPageManager = new DataPageManager(tableDir, myBuffer, new BPlusEngineConf(3, 1022, 1022));
//
//        System.out.println("Running");
//        int records = 10_000_000;
//        {
//            AtomicInteger cntPut = new AtomicInteger(0);
//            Function<Integer, String> func = i -> {
//                CachedPageNew<SlottedPage> cachedPage;
//                try {
//                    cachedPage = dataPageManager.getAndLockWriteFreePage();
//                    long pageId = cachedPage.pageId;
//                    short slotId;
//                    {
//                        SlottedPage page = cachedPage.getPage();
//                        String key = "key" + String.format("%07d", i);;
//                        String value = "value" + String.format("%07d", i);;
//                        slotId = page.addItem(new KeyValueItem(key, value));
//                        dataPageManager.unlockPage(cachedPage);
//                    }
//
//
////                     read
//                    {
//
//                        cachedPage = dataPageManager.getAndLockReadPage(pageId);
//                        String key = "key" + String.format("%07d", i);
//                        String value = "value" + String.format("%07d", i);
//                        SlottedPage page = cachedPage.getPage();
//                        KeyValueItem keyValueItem = new KeyValueItem(page.getBytesReadOnly(slotId));
//                        assertEquals(key, keyValueItem.key);
//                        assertEquals(value, keyValueItem.value);
//                        dataPageManager.unlockPage(cachedPage);
//                    }
//
//                    if (i % 2 == 0) {
//                        // update
//                        {
//
//                            cachedPage = dataPageManager.getAndLockWritePage(pageId);
//                            String key = "key" + String.format("%07d", i);
//                            String value = "value";
//                            SlottedPage page = cachedPage.getPage();
//                            page.update(slotId, new KeyValueItem(key, value));
//                            dataPageManager.unlockPage(cachedPage);
//                        }
//
//                        // read
//                        {
//
//                            cachedPage = dataPageManager.getAndLockReadPage(pageId);
//                            String key = "key" + String.format("%07d", i);
//                            String value = "value";
//                            SlottedPage page = cachedPage.getPage();
//                            KeyValueItem keyValueItem = new KeyValueItem(page.getBytesReadOnly(slotId));
//                            assertEquals(key, keyValueItem.key);
//                            assertEquals(value, keyValueItem.value);
//                            dataPageManager.unlockPage(cachedPage);
//                        }
//                    }
//
////                    if (i % 3 == 0) {
////                        // delete
////                        {
////                            ;
////                            try {
////                                cachedPage = dataPageManager.getAndLockWritePage(pageId);
////                                SlottedPage page = cachedPage.getPage();
////                                boolean wasFull = page.isFull();
////                                page.delete(slotId);
//////                            System.out.println("Deleting Page: " + pageId + " P: " + page.pageId + "  Slot: " + slotId);
//////                                if (wasFull && !page.isFull() && cachedPage.getInUseCnt() == 1) {
//////                                    cachedPage.lock();
//////                                    if (!page.isFull() && cachedPage.getInUseCnt() == 1) {
//////
//////                                        freePages.put(pageId, true);
//////
////////                                        printThread("Deleting CCC PageId: " + pageId + " SlotId: " + slotId);
////////                                        pagesManager.setDataPageAsFree(pageId);
////////                                        printThread("Deleted CCC PageId: " + pageId + " SlotId: " + slotId);
//////                                    }
//////
//////
//////                                }
////
////                                dataPageManager.unlockPage(cachedPage);
////                            } finally {
//////                                spaceLock.writeLock().unlock();
//////                                printThread("Return space lock");
////                            }
////
////                        }
////                    }
//                    int ccc = cntPut.incrementAndGet();
//                    if (ccc % 100_000 == 0) System.err.println(ccc);
//                    return "";
//                } catch (Throwable e) {
//                    e.printStackTrace();
//                    System.exit(1);
//                    throw new RuntimeException("");
//                }
//            };
//            runConcurrentTest(records, 1000, 1, func, numberOfThreads);
//        }
//
//
//    }

}
