package com.keks.kv_storage.bplus.page_manager.managers;


import com.keks.kv_storage.bplus.FreeSpaceCheckerInter;
import com.keks.kv_storage.bplus.bitmask.BitMaskRingCache;
import com.keks.kv_storage.bplus.buffer.CachedPageNew;
import com.keks.kv_storage.bplus.buffer.PageBuffer;
import com.keks.kv_storage.bplus.conf.BPlusConf;
import com.keks.kv_storage.record.KVRecord;
import com.keks.kv_storage.bplus.item.KVRecordSplitsBuilder;
import com.keks.kv_storage.bplus.item.KvRecordSplit;
import com.keks.kv_storage.bplus.item.KVRecordSplitter;
import com.keks.kv_storage.bplus.page_manager.PageManager;
import com.keks.kv_storage.bplus.page_manager.page_disk.sp.SlotLocation;
import com.keks.kv_storage.bplus.page_manager.page_disk.sp.SlottedPage;
import com.keks.kv_storage.bplus.page_manager.page_key.PageKeyBuilder;
import com.keks.kv_storage.bplus.page_manager.page_key.PageType;
import com.keks.kv_storage.bplus.page_manager.pageio.SlottedPageIO;
import com.keks.kv_storage.bplus.tree.KeyToDataLocationsItem;
import com.keks.kv_storage.bplus.tree.node.leaf.LeafDataLocation;

import java.io.File;
import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;


public class DataPageManager extends PageManager<SlottedPage> {
//    final static Logger logger = Logger.getLogger(DataPageManager.class);
    private final ConcurrentHashMap<Long, Boolean> canBeMarkedAsFree = new ConcurrentHashMap<>();

    private final PageBuffer myBuffer;
    public final FreeSpaceCheckerInter freeSpaceChecker;
    private final SlottedPageIO slottedPageIO;
    private final static String FILE_NAME = "data.db";

    private final PageKeyBuilder pageKeyBuilder = new PageKeyBuilder(PageType.DATA_PAGE_TYPE);

    public DataPageManager(File tableDir,
                           PageBuffer myBuffer,
                           BPlusConf bplusConf) throws IOException {
        PageKeyBuilder pageKeyBuilder = new PageKeyBuilder(PageType.DATA_PAGE_TYPE);
        this.myBuffer = myBuffer;
        this.slottedPageIO = new SlottedPageIO(tableDir, FILE_NAME);
        this.freeSpaceChecker = new BitMaskRingCache<>(bplusConf.freeSpaceCheckerMaxCache, bplusConf.freeSpaceCheckerInitCache, myBuffer, slottedPageIO, pageKeyBuilder);
    }

    @Override
    public CachedPageNew<SlottedPage> getAndLockWriteFreePage() throws IOException {
        printThread("trying to get free page from cache");
        CachedPageNew<SlottedPage> cachedPage = null;
//        cachedPage = myBuffer.tryToGetCachedFreePage(
//                freeSpaceChecker,
//                pageKeyBuilder.pageType);

        if (null == cachedPage) {
            printThread("trying to get free page ");
            long nextFreePage = freeSpaceChecker.nextFreePage();
            cachedPage = myBuffer.getAndLockWrite(pageKeyBuilder.getPageKey(nextFreePage), slottedPageIO);
            if (cachedPage.getPage().isFull()) {
                printThread("error Page: " + cachedPage.pageId);
            }
//            cachedPage.sb.append("Thread[" + Thread.currentThread().getId() + "]  " + "PageId: " + cachedPage.pageId + " Free loaded by specific ID InUseCnt" + cachedPage.getInUseCnt() + "\n");
            assert !cachedPage.getPage().isFull();
        } else {
//            cachedPage.sb.append("Thread[" + Thread.currentThread().getId() + "]  " + "PageId: " + cachedPage.pageId + " Free Loaded from buffer try" + "InUseCnt" + cachedPage.getInUseCnt() + "\n");
            assert !cachedPage.getPage().isFull();
        }
//        cachedPage.threads.add(Thread.currentThread().getId());
        canBeMarkedAsFree.put(cachedPage.pageId, true);
        return cachedPage;
    }

    @Override
    public CachedPageNew<SlottedPage> getAndLockWritePage(long pageId) throws IOException {
        printThread("taking specific page for write " + pageId);

        CachedPageNew<SlottedPage> cachedPage = myBuffer.getAndLockWrite(pageKeyBuilder.getPageKey(pageId), slottedPageIO);
//        cachedPage.sb.append("Thread[" + Thread.currentThread().getId() + "]  " + "PageId: " + cachedPage.pageId + " Loaded with write by specific ID" + "InUseCnt" + cachedPage.getInUseCnt() + "\n");
//        cachedPage.threads.add(Thread.currentThread().getId());
        canBeMarkedAsFree.put(cachedPage.pageId, cachedPage.getPage().isFull());
        return cachedPage;
    }

    @Override
    public CachedPageNew<SlottedPage> getAndLockReadPage(long pageId) throws IOException {
        printThread("taking specific page for read" + pageId);
        CachedPageNew<SlottedPage> cachedPage = myBuffer.getAndLockRead(pageKeyBuilder.getPageKey(pageId), slottedPageIO);
//        cachedPage.sb.append("Thread[" + Thread.currentThread().getId() + "]  " + "PageId: " + cachedPage.pageId + " Loaded read by specific ID " + "InUseCnt" + cachedPage.getInUseCnt() + "\n");
//        cachedPage.threads.add(Thread.currentThread().getId());
        return cachedPage;
    }

    @Override
    public int isPageFree(long pageId) {
        return freeSpaceChecker.isFree(pageId);
    }

    @Override
    public void setForceNotFreePage(long pageId) {
        freeSpaceChecker.setForceNotFree(pageId);
    }

    @Override
    public void unlockPage(CachedPageNew<SlottedPage> cachedPage) throws IOException {
        long pageId = cachedPage.pageId;
        if (cachedPage.pageLock.isWriteLocked() && cachedPage.getPage().isFull()) { // TODO uncomment and test 1000 times
            cachedPage.getPage().tryToOptimize();
        }
        boolean isFree = !cachedPage.getPage().isFull();

        if (isFree && Optional.ofNullable(canBeMarkedAsFree.remove(pageId)).orElse(false)) {
            freeSpaceChecker.setFree(pageId);
        }
//        printThread("releasing page " + cachedPage.pageId + " InUseCnt" + cachedPage.getInUseCnt());
        myBuffer.unLock(cachedPage);
//        printThread("released page " + cachedPage.pageId + " InUseCnt" + cachedPage.getInUseCnt());
    }

    @Override
    public String printFreeSpaceInfo() {
//        return freeSpaceChecker.printCurrentPageInfo();
        return "";
    }

    @Override
    public void close() throws IOException {
        freeSpaceChecker.close();
        slottedPageIO.close();
    }

    public LeafDataLocation[] addNewKeyDataTuple(KVRecord keyDataTuple) throws IOException {
        KVRecordSplitter splitter = new KVRecordSplitter(keyDataTuple);
        ArrayList<LeafDataLocation> locations = new ArrayList<>(2); // item locations in data file
        while (splitter.hasNextSplit()) {
            CachedPageNew<SlottedPage> freeDataPage = getAndLockWriteFreePage();
            try {
                SlottedPage page = freeDataPage.getPage();
                KvRecordSplit kvRecordSplit;
                try {
                    kvRecordSplit = splitter.nextSplit(page.getFreeSpace());
                } catch (BufferOverflowException t) {

                    throw t;
                }


                short slotId = page.addItem(kvRecordSplit);
                locations.add(new LeafDataLocation(freeDataPage.pageId, slotId));
            } finally {
                unlockPage(freeDataPage);
            }
        }
        LeafDataLocation[] locationsArr = new LeafDataLocation[locations.size()];
        locationsArr = locations.toArray(locationsArr);
        return locationsArr;
    }

    private ByteBuffer getDataPageBytes(SlotLocation location) throws IOException {
        CachedPageNew<SlottedPage> cachedPage = getAndLockReadPage(location.pageId);
        try {
            SlottedPage page = cachedPage.getPage();
            return page.getBytesReadOnly(location.slotId);
        } finally {
            unlockPage(cachedPage);
        }
    }

    public KVRecord getKeyDataTuple(KeyToDataLocationsItem dataLocation) throws IOException {

        KVRecordSplitsBuilder recordSplitsBuilder = new KVRecordSplitsBuilder(dataLocation.dataLen);
        for (SlotLocation location : dataLocation.dataLocations) {
            recordSplitsBuilder.add(getDataPageBytes(location));
        }

        return recordSplitsBuilder.buildKVRecord();
    }

    public void deleteKeyDataTuple(KeyToDataLocationsItem dataLocation) throws IOException {
        for (SlotLocation location : dataLocation.dataLocations) {
            CachedPageNew<SlottedPage> dataPage = getAndLockWritePage(location.pageId);
            try {
                SlottedPage page = dataPage.getPage();
                page.delete(location.slotId);
            } finally {
                unlockPage(dataPage);
            }
        }
    }

    public LeafDataLocation[] replaceData(LeafDataLocation[] previousLocations, KVRecord newKeyDataTuple) throws IOException {
        KVRecordSplitter newSplitter = new KVRecordSplitter(newKeyDataTuple);
        ArrayList<LeafDataLocation> addLocations = new ArrayList<>();
        for (LeafDataLocation location : previousLocations) {
            CachedPageNew<SlottedPage> dataPage = getAndLockWritePage(location.pageId);
            try {
                SlottedPage page = dataPage.getPage();
                if (newSplitter.hasNextSplit()) {
                    int previousItemLength = page.getItemLength(location.slotId);
                    KvRecordSplit next = newSplitter.nextSplit(previousItemLength);
                    page.update(location.slotId, next);
                    addLocations.add(location);
                } else {
                    page.delete(location.slotId);
                }
            } finally {
                unlockPage(dataPage);
            }
        }
        while (newSplitter.hasNextSplit()) {
            CachedPageNew<SlottedPage> dataPage = getAndLockWriteFreePage();
            try {
                SlottedPage page = dataPage.getPage();
                KvRecordSplit next = newSplitter.nextSplit(page.getFreeSpace());
                short slotId = page.addItem(next);
                addLocations.add(new LeafDataLocation(page.pageId, slotId));
            } finally {
                unlockPage(dataPage);
            }
        }
        LeafDataLocation[] newLocationsArr = new LeafDataLocation[addLocations.size()];
        newLocationsArr = addLocations.toArray(newLocationsArr);
        return newLocationsArr;
    }

    public void printStatistics() {
//        System.out.println("LockedByThreads: " + freeSpaceChecker.getLockedByThreadsCnt());
    }

    public String printPagesInfo() throws IOException {
        long fileSize = slottedPageIO.getFileSize();
        return String.valueOf(fileSize);
    }

//    public ArrayList<PageStatistics> getPagesStatistics() throws IOException {
//        ArrayList<PageStatistics> pagesStatistics = new ArrayList<>((int) slottedPageIO.getPagesCnt());
//        Iterator<SlottedPage> allPages = slottedPageIO.getAllPages();
//        while (allPages.hasNext()) {
//            SlottedPage next = allPages.next();
//            if (!freeSpaceChecker.isPageInRange(next.pageId)) {
//                freeSpaceChecker.forceLoadNextBitMaskPage();
//            }
//            assert freeSpaceChecker.isFree(next.pageId) != -1;
//            PageStatistics pageStatistics = new PageStatistics(
//                    next.pageId,
//                    next.getNextSlotId(),
//                    freeSpaceChecker.isFree(next.pageId) == 1,
//                    next.getFreeSpace());
//            pagesStatistics.add(pageStatistics);
//        }
//        return pagesStatistics;
//    }

    public static void printThread(String msg) {
//        System.out.println("Thread[" + Thread.currentThread().getId() + "]  DataPageManager: " + msg);
//        logger.debug("Thread[" + Thread.currentThread().getId() + "]: " + msg);
    }


}
