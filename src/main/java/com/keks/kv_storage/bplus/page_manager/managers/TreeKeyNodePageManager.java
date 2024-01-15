package com.keks.kv_storage.bplus.page_manager.managers;

import com.keks.kv_storage.bplus.FreeSpaceCheckerInter;
import com.keks.kv_storage.bplus.bitmask.BitMaskRingCache;
import com.keks.kv_storage.bplus.buffer.CachedPageNew;
import com.keks.kv_storage.bplus.buffer.PageBuffer;
import com.keks.kv_storage.bplus.conf.BPlusConf;
import com.keks.kv_storage.bplus.page_manager.BplusTreeRuntimeParameters;
import com.keks.kv_storage.bplus.page_manager.PageIO;
import com.keks.kv_storage.bplus.page_manager.page_key.PageKeyBuilder;
import com.keks.kv_storage.bplus.page_manager.page_key.PageType;
import com.keks.kv_storage.bplus.page_manager.pageio.TreeKeyNodePageIO;
import com.keks.kv_storage.bplus.tree.node.TreeNode;
import com.keks.kv_storage.bplus.tree.node.key.TreeKeyNode;
import com.keks.kv_storage.bplus.tree.node.traverse.TraverseKeyNode;

import java.io.File;
import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;


public class TreeKeyNodePageManager {

    private final ConcurrentHashMap<Long, Boolean> canBeMarkedAsFree = new ConcurrentHashMap<>();

    private final PageBuffer myBuffer;
    public final FreeSpaceCheckerInter freeSpaceChecker;
    public final static String FILE_NAME = "tree-key.db";

    public final BplusTreeRuntimeParameters bplusTreeRuntimeParameters;

    private final PageKeyBuilder pageKeyBuilder = new PageKeyBuilder(PageType.TREE_KEY_NODE_PAGE_TYPE);

    public final BPlusConf bPlusConf;
    private final TreeKeyNodePageIO treeKeyNodePageIO;


    public TreeKeyNodePageManager(BplusTreeRuntimeParameters bplusTreeRuntimeParameters,
                                  BPlusConf bPlusConf,
                                  File dataDirName,
                                  PageBuffer myBuffer) throws IOException {
        this.bplusTreeRuntimeParameters = bplusTreeRuntimeParameters;
        this.bPlusConf = bPlusConf;
        this.myBuffer = myBuffer;
        this.treeKeyNodePageIO = new TreeKeyNodePageIO(dataDirName, FILE_NAME, bPlusConf.btreeConf);
        this.freeSpaceChecker = new BitMaskRingCache<>(bPlusConf.freeSpaceCheckerMaxCache, bPlusConf.freeSpaceCheckerInitCache, myBuffer, treeKeyNodePageIO, pageKeyBuilder);
        // when loading empty page all numbers are zero by default and this can lead to unpredictable behaviors
        // for example left sibling doesn't exist but empty page contains leftSibling as 0 page. So no one should use 0 page
        this.freeSpaceChecker.setForceNotFree(0L);
    }

    public CachedPageNew<TreeKeyNode> getAndLockWriteFreeKeyNodePage() throws IOException {
        CachedPageNew<TreeKeyNode> cachedPage = getAndLockWriteFreePage(treeKeyNodePageIO);
        return cachedPage;
    }

    private <T extends TreeNode<?>> CachedPageNew<T> getAndLockWriteFreePage(PageIO<T> pageIO) throws IOException {
        CachedPageNew<T> cachedPage = null;
//        cachedPage = myBuffer.tryToGetCachedFreePage(
//                freeSpaceChecker,
//                pageKeyBuilder.pageType);
        if (null == cachedPage) {
            long nextFreePage = freeSpaceChecker.nextFreePage();
            cachedPage = myBuffer.getAndLockWrite(pageKeyBuilder.getPageKey(nextFreePage), pageIO);
        }
        cachedPage.getPage().setTreeOrder(bPlusConf.btreeConf.treeOrder);
        assert !cachedPage.getPage().isFull();
        canBeMarkedAsFree.put(cachedPage.pageId, true);
        return cachedPage;
    }

    public CachedPageNew<TreeKeyNode> getAndLockWriteKeyNodePage(long pageId) throws IOException {
        return getAndLockWritePage(pageId, treeKeyNodePageIO);
    }

    private <T extends TreeNode<?>> CachedPageNew<T> getAndLockWritePage(long pageId, PageIO<T> pageIO) throws IOException {
        CachedPageNew<T> cachedPage = myBuffer.getAndLockWrite(pageKeyBuilder.getPageKey(pageId), pageIO);
        canBeMarkedAsFree.put(cachedPage.pageId, cachedPage.getPage().isFull());
        return cachedPage;
    }


    public CachedPageNew<TreeKeyNode> getAndLockReadKeyNodePage(long pageId) throws IOException {
        return getAndLockReadPage(pageId, treeKeyNodePageIO);
    }


    private  <T extends TreeNode<?>> CachedPageNew<T> getAndLockReadPage(long pageId, PageIO<T> pageIO) throws IOException {
//        PageBuffer.printThread("Lock Read: " + pageId, true);
        return myBuffer.getAndLockRead(pageKeyBuilder.getPageKey(pageId), pageIO);
    }


    public int isPageFree(long pageId) {
        return freeSpaceChecker.isFree(pageId);
    }

    public void setForceNotFreePage(long pageId) {
        freeSpaceChecker.setForceNotFree(pageId);
    }

    public <T extends TreeNode<?>> void unlockPage(CachedPageNew<TreeKeyNode> cachedPage) throws IOException {
        long pageId = cachedPage.pageId;
//        if (cachedPage.pageLock.isWriteLocked() && cachedPage.getPage().isFull()) { // TODO uncomment and test 1000 times
//            cachedPage.getPage().tryToOptimize();
//        }
        boolean isFree = !cachedPage.getPage().isFull();

        // TODO do not uncomment if page was read from disk and even be empty after time  this page cannot be used again since in buf it is a key or leaf
        if (isFree && Optional.ofNullable(canBeMarkedAsFree.remove(pageId)).orElse(false)) {
            freeSpaceChecker.setFree(pageId);

        }
        myBuffer.unLock(cachedPage);
    }

    public void close() throws IOException {
        freeSpaceChecker.close();
        treeKeyNodePageIO.close();
        bplusTreeRuntimeParameters.close();
    }

    public void unlockTraverseItem(TraverseKeyNode item) throws IOException {
        unlockPage(item.nodePage);
        if (item.getLeftSiblingPage() != null) unlockPage(item.getLeftSiblingPage());
        if (item.getRightSiblingPage() != null) unlockPage(item.getRightSiblingPage());
    }

    public String printFreeSpaceInfo() {
        return "";
    }

    public String printSpecTreeNodeChecker() {
        return "";
    }

}
