package com.keks.kv_storage.bplus.page_manager.managers;

import com.keks.kv_storage.bplus.FreeSpaceCheckerInter;
import com.keks.kv_storage.bplus.buffer.CachedPageNew;
import com.keks.kv_storage.bplus.buffer.PageBuffer;
import com.keks.kv_storage.bplus.bitmask.BitMaskRingCache;
import com.keks.kv_storage.bplus.conf.BPlusConf;
import com.keks.kv_storage.bplus.page_manager.page_key.PageKeyBuilder;
import com.keks.kv_storage.bplus.page_manager.PageManager;
import com.keks.kv_storage.bplus.page_manager.page_key.PageType;
import com.keks.kv_storage.bplus.page_manager.page_disk.sp.SlottedPage;
import com.keks.kv_storage.bplus.page_manager.pageio.SlottedPageIO;
import com.keks.kv_storage.bplus.item.KeyItem;
import com.keks.kv_storage.bplus.tree.KeyToDataLocationsItem;
import com.keks.kv_storage.bplus.tree.node.key.KeyLocation;
import com.keks.kv_storage.bplus.tree.node.leaf.LeafDataLocation;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;


public class IndexPageManager extends PageManager<SlottedPage> {

    private final ConcurrentHashMap<Long, Boolean> canBeMarkedAsFree = new ConcurrentHashMap<>();
    private final PageBuffer myBuffer;
    public final FreeSpaceCheckerInter freeSpaceChecker;
    private final SlottedPageIO slottedPageIO;
    public final static String FILE_NAME = "index.db";

    private final PageKeyBuilder pageKeyBuilder = new PageKeyBuilder(PageType.INDEX_PAGE_TYPE);

    public IndexPageManager(File tableDir,
                            PageBuffer myBuffer,
                            BPlusConf bplusConf) throws IOException {
        this.myBuffer = myBuffer;
        this.slottedPageIO = new SlottedPageIO(tableDir, FILE_NAME);
        this.freeSpaceChecker = new BitMaskRingCache<>(bplusConf.freeSpaceCheckerMaxCache, bplusConf.freeSpaceCheckerInitCache, myBuffer, slottedPageIO, pageKeyBuilder);
    }

    @Override
    public CachedPageNew<SlottedPage> getAndLockWriteFreePage() throws IOException {
        CachedPageNew<SlottedPage> cachedPage = null;
//        cachedPage = myBuffer.tryToGetCachedFreePage(
//                freeSpaceChecker,
//                pageKeyBuilder.pageType);

        if (null == cachedPage) {
            long nextFreePage = freeSpaceChecker.nextFreePage();
            cachedPage = myBuffer.getAndLockWrite(pageKeyBuilder.getPageKey(nextFreePage), slottedPageIO);
        }
        assert !cachedPage.getPage().isFull();
        canBeMarkedAsFree.put(cachedPage.pageId, true);
        return cachedPage;
    }


    @Override
    public CachedPageNew<SlottedPage> getAndLockWritePage(long pageId) throws IOException {
        CachedPageNew<SlottedPage> cachedPage = myBuffer.getAndLockWrite(pageKeyBuilder.getPageKey(pageId), slottedPageIO);
        canBeMarkedAsFree.put(cachedPage.pageId, cachedPage.getPage().isFull());
        return cachedPage;
    }

    @Override
    public CachedPageNew<SlottedPage> getAndLockReadPage(long pageId) throws IOException {
        return myBuffer.getAndLockRead(pageKeyBuilder.getPageKey(pageId), slottedPageIO);
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
//        if (cachedPage.pageLock.isWriteLocked() && cachedPage.getPage().isFull()) { // TODO uncomment and test 1000 times
//            cachedPage.getPage().tryToOptimize();
//        }
        boolean isFree = !cachedPage.getPage().isFull();

        if (isFree && Optional.ofNullable(canBeMarkedAsFree.remove(pageId)).orElse(false)) {
            freeSpaceChecker.setFree(pageId);
        }
        myBuffer.unLock(cachedPage);
    }


    @Override
    public void close() throws IOException {
        freeSpaceChecker.close();
        slottedPageIO.close();
    }

    // TODO check also before the last slot since data could be deleted before last slot and free space occurs before last slot
    public CachedPageNew<SlottedPage> getFreePageWhichCanStore(int bytesToStore) throws IOException {
        ArrayList<CachedPageNew<SlottedPage>> pagesToUnLock = new ArrayList<>(1); // all pages are set as full until find required page
        boolean search = true;
        CachedPageNew<SlottedPage> cachedPage;
        do {
            cachedPage = getAndLockWriteFreePage();
            SlottedPage slottedPage = cachedPage.getPage();
            if (slottedPage.canStore(bytesToStore)) {
                search = false;
            } else {
                pagesToUnLock.add(cachedPage);
            }

        } while (search);
        for (CachedPageNew<SlottedPage> slottedPageCachedPage : pagesToUnLock) {
            unlockPage(slottedPageCachedPage);
        }
        return cachedPage;
    }

    public KeyToDataLocationsItem getKeyToDataLocation(LeafDataLocation leafDataLocation) throws IOException {
        CachedPageNew<SlottedPage> cachedPage = getAndLockReadPage(leafDataLocation.pageId);
        try {
            SlottedPage page = cachedPage.getPage();
            return new KeyToDataLocationsItem(page.getBytesReadOnly(leafDataLocation.slotId));
        } finally {
            unlockPage(cachedPage);
        }
    }

    public String getKey(KeyLocation location) throws IOException {
        CachedPageNew<SlottedPage> cachedPage = getAndLockReadPage(location.pageId);
        try {
            return new KeyItem(cachedPage.getPage().getBytesReadOnly(location.slotId)).key;
        } finally {
            unlockPage(cachedPage);
        }
    }

    public LeafDataLocation addLeafDataLocations(KeyToDataLocationsItem keyToDataLocations) throws IOException {
        CachedPageNew<SlottedPage> indexCachedPage = getFreePageWhichCanStore(keyToDataLocations.getLen());
        try {
            SlottedPage slottedPage = indexCachedPage.getPage();
            long pageId = slottedPage.pageId;
            short slotId = slottedPage.addItem(keyToDataLocations);
            return new LeafDataLocation(pageId, slotId);
        } finally {
            unlockPage(indexCachedPage);
        }
    }

    public LeafDataLocation updateKeyToDataLocations(LeafDataLocation previousLeafDataLocation,
                                                     int previousDataLen,
                                                     KeyToDataLocationsItem newKeyToDataLocations) throws IOException {
//        DataBlockItem newLeafDataBlock = new DataBlockItem(newKeyToDataLocations.getBytes());
        if (newKeyToDataLocations.getLen() <= previousDataLen) {
            CachedPageNew<SlottedPage> cachedPage = getAndLockWritePage(previousLeafDataLocation.pageId);
            try {
                SlottedPage page = cachedPage.getPage();
                page.update(previousLeafDataLocation.slotId, newKeyToDataLocations);
            } finally {
                unlockPage(cachedPage);
            }
            return previousLeafDataLocation;
        } else {
            deleteLeafDataLocation(previousLeafDataLocation);
            return addLeafDataLocations(newKeyToDataLocations);
        }
    }

    public KeyToDataLocationsItem deleteLeafDataLocation(LeafDataLocation location) throws IOException {
        CachedPageNew<SlottedPage> cachedPage = getAndLockWritePage(location.pageId);
        try {
            SlottedPage page = cachedPage.getPage();
            KeyToDataLocationsItem res = new KeyToDataLocationsItem(page.getBytesReadOnly(location.slotId));
            page.delete(location.slotId);
            return res;
        } finally {
            unlockPage(cachedPage);
        }
    }

    public KeyItem deleteKey(KeyLocation location) throws IOException {
        CachedPageNew<SlottedPage> cachedPage = getAndLockWritePage(location.pageId);
        try {
            SlottedPage page = cachedPage.getPage();
            KeyItem res = new KeyItem(page.getBytesReadOnly(location.slotId));
            page.delete(location.slotId);
            return res;
        } finally {
            unlockPage(cachedPage);
        }
    }

    public KeyLocation addIndexedKey(KeyItem key) throws IOException {
        CachedPageNew<SlottedPage> indexCachedPage = getFreePageWhichCanStore(key.getLen());
        try {
            SlottedPage slottedPage = indexCachedPage.getPage();
            long pageId = slottedPage.pageId;
            short slotId = slottedPage.addItem(key);
            return new KeyLocation(pageId, slotId);
        } finally {
            unlockPage(indexCachedPage);
        }
    }

    @Override
    public String printFreeSpaceInfo() {
        return "";
    }

    public String printPagesInfo() throws IOException {
        long fileSize = slottedPageIO.getFileSize();
        return String.valueOf(fileSize);
    }

}
