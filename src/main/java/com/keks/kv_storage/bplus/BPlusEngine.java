package com.keks.kv_storage.bplus;

import com.keks.kv_storage.bplus.page_manager.page_disk.fixed.TreeNodePage;
import com.keks.kv_storage.record.KVRecord;
import com.keks.kv_storage.bplus.buffer.CachedPageNew;
import com.keks.kv_storage.bplus.buffer.PageBuffer;
import com.keks.kv_storage.bplus.conf.BPlusConf;
import com.keks.kv_storage.bplus.page_manager.BplusTreeRuntimeParameters;
import com.keks.kv_storage.bplus.page_manager.managers.DataPageManager;
import com.keks.kv_storage.bplus.page_manager.managers.IndexPageManager;
import com.keks.kv_storage.bplus.page_manager.managers.TreeKeyNodePageManager;
import com.keks.kv_storage.bplus.page_manager.managers.TreeLeafNodePageManager;
import com.keks.kv_storage.bplus.tree.KeyToDataLocationsItem;
import com.keks.kv_storage.bplus.tree.node.key.TreeKeyNode;
import com.keks.kv_storage.bplus.tree.node.leaf.TreeLeafNode;
import com.keks.kv_storage.bplus.tree.node.traverse.TraverseLeafNode;
import com.keks.kv_storage.bplus.tree.node.traverse.TraverseNodes;
import com.keks.kv_storage.bplus.tree.utils.BPlusKVTableDeleteUtils;
import com.keks.kv_storage.bplus.tree.utils.BPlusKVTableInsertUtils;
import com.keks.kv_storage.bplus.tree.utils.BPlusKVTableUtils;
import com.keks.kv_storage.conf.Params;
import com.keks.kv_storage.conf.TableEngine;
import com.keks.kv_storage.ex.NotImplementedException;
import com.keks.kv_storage.io.JsonFileRW;
import com.keks.kv_storage.query.Query;
import com.keks.kv_storage.query.QueryIterator;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;


public class BPlusEngine extends TableEngine {

    public final TreeKeyNodePageManager treeKeyNodePageManager;
    private final TreeLeafNodePageManager treeLeafNodePageManager;
    public final IndexPageManager indexPageManager;
    public final DataPageManager dataPageManager;
    private final BPlusConf bplusConf;
    private static final long ZERO_PAGE = 0L;
    private final ReentrantReadWriteLock rootLock = new ReentrantReadWriteLock();

    private final PageBuffer myBuffer;


    public static BPlusEngine createNewTable(String tableId,
                                             File dataDir,
                                             BPlusConf bplusConf) throws IOException {
        JsonFileRW.write(new File(dataDir, "tree-params"), bplusConf.getAsJson());
        BplusTreeRuntimeParameters bplusTreeRuntimeParameters = new BplusTreeRuntimeParameters(bplusConf.btreeConf.treeOrder, dataDir);
        long pageBufferMaxPages = bplusConf.pageBufferSizeBytes / TreeNodePage.DEFAULT_PAGE_SIZE;
        PageBuffer pageBuffer = new PageBuffer((int) pageBufferMaxPages);
        TreeKeyNodePageManager treeKeyNodePageManager =
                new TreeKeyNodePageManager(bplusTreeRuntimeParameters, bplusConf, dataDir, pageBuffer);
        TreeLeafNodePageManager treeLeafNodePageManager =
                new TreeLeafNodePageManager(bplusTreeRuntimeParameters, bplusConf, dataDir, pageBuffer);

        return new BPlusEngine(tableId, dataDir, treeKeyNodePageManager, treeLeafNodePageManager, bplusConf, pageBuffer);
    }

    public static BPlusEngine loadTable(String tableId,
                                        File dataDir) throws IOException {
        BplusTreeRuntimeParameters bplusTreeRuntimeParameters = new BplusTreeRuntimeParameters(dataDir);
        BPlusConf bPlusConf = new BPlusConf(JsonFileRW.read(new File(dataDir, "tree-params")));

        long pageBufferMaxPages = bPlusConf.pageBufferSizeBytes / TreeNodePage.DEFAULT_PAGE_SIZE;
        PageBuffer pageBuffer = new PageBuffer((int) pageBufferMaxPages);
        TreeKeyNodePageManager treeKeyNodePageManager = new TreeKeyNodePageManager(bplusTreeRuntimeParameters, bPlusConf, dataDir, pageBuffer);
        TreeLeafNodePageManager treeLeafNodePageManager = new TreeLeafNodePageManager(bplusTreeRuntimeParameters, bPlusConf, dataDir, pageBuffer);

        // during adding, deleting and restarting db pages can be used but be free and freeSpaceChecker could think that they are free
//        treeNodePageManager.setForceNotFreePage(treeNodePageManager.treeProperties.getRootPageId());
//        treeNodePageManager.setForceNotFreePage(treeNodePageManager.treeProperties.getMostLeftLeafPageId());

//        BtreeOrderConf btreeParams = new BtreeOrderConf(treeNodePageManager.bplusTreeRuntimeParameters.getBtreeOrder());

        return new BPlusEngine(tableId, dataDir, treeKeyNodePageManager, treeLeafNodePageManager, bPlusConf, pageBuffer);
    }


    private BPlusEngine(String tableId,
                        File dataDir,
                        TreeKeyNodePageManager treeKeyNodePageManager,
                        TreeLeafNodePageManager treeLeafNodePageManager,
                        BPlusConf bplusConf,
                        PageBuffer myBuffer) throws IOException {
        this.treeKeyNodePageManager = treeKeyNodePageManager;
        this.treeLeafNodePageManager = treeLeafNodePageManager;
        this.indexPageManager = new IndexPageManager(dataDir, myBuffer, bplusConf);
        this.dataPageManager = new DataPageManager(dataDir, myBuffer, bplusConf);
        this.bplusConf = bplusConf;
        this.myBuffer = myBuffer;
    }


    public AtomicInteger cntReadAdd = new AtomicInteger();
    public AtomicInteger cntWriteAdd = new AtomicInteger();

    @Override
    public void putKV(byte[] key, byte[] value) throws IOException {
        addWithWriteLock(new KVRecord(new String(key), value));
    }

    @Override
    public void put(KVRecord kvRecord) throws IOException {
        addWithWriteLock(kvRecord);
    }

    @Override
    public void remove(String key) throws IOException {
        deleteWithWriteLock(key);
    }

    public void addWithWriteLock(KVRecord keyDataTuple) throws IOException {
        rootLock.writeLock().lock();
//        printThread("Add Write Key: " + keyDataTuple.key, true);
        long rootPageId = treeKeyNodePageManager.bplusTreeRuntimeParameters.getRootPageId();
        if (rootPageId > ZERO_PAGE) { // root exist

            final CachedPageNew<TreeKeyNode> rootPage = treeKeyNodePageManager.getAndLockWriteKeyNodePage(rootPageId);

            TraverseNodes traverseNodes = BPlusKVTableInsertUtils.traverseDownWrite(
                    rootPage,
                    keyDataTuple.key,
                    indexPageManager,
                    treeKeyNodePageManager,
                    treeLeafNodePageManager,
                    rootLock);

            BPlusKVTableInsertUtils.addToLeafAndTraverseUp(
                    keyDataTuple,
                    indexPageManager,
                    treeKeyNodePageManager,
                    treeLeafNodePageManager,
                    dataPageManager,
                    traverseNodes,
                    rootLock);
        } else {
            try {
                long mostLeftLeafPageId = treeKeyNodePageManager.bplusTreeRuntimeParameters.getMostLeftLeafPageId();
                if (mostLeftLeafPageId <= ZERO_PAGE) {
                    mostLeftLeafPageId = 1;
                    treeKeyNodePageManager.bplusTreeRuntimeParameters.incTreeHeight();
//                    printThread("TreeHeight: Add write Key (Added new most leaf since prev was null): " + keyDataTuple.key + " increased tree height to: "
//                            + treeNodePageManager.treeHeader.getTreeHeight(), true);
                    treeKeyNodePageManager.bplusTreeRuntimeParameters.setMostLeftLeafPageId(mostLeftLeafPageId);
                    treeLeafNodePageManager.setForceNotFreePage(mostLeftLeafPageId);
                }
                CachedPageNew<TreeLeafNode> mostLeafCachedPage = treeLeafNodePageManager.getAndLockWriteLeafNodePage(mostLeftLeafPageId);

                TreeLeafNode mostLeafNode = mostLeafCachedPage.page;
                BPlusKVTableInsertUtils.addToLeaf(mostLeafNode, keyDataTuple, indexPageManager, dataPageManager);
                if (mostLeafNode.leafKeys.isFull()) {
                    CachedPageNew<TreeKeyNode> newRootKeyNodePage = treeKeyNodePageManager.getAndLockWriteFreeKeyNodePage();
//                    printThread("New Root1: " + newRootKeyNodePage.pageId, true);
//                    printThread("leaf to split: " + mostLeafNode.toString1(indexPageManager), true);
                    BPlusKVTableInsertUtils.splitLeaf(
                            true,
                            newRootKeyNodePage.page,
                            new TraverseLeafNode(mostLeafCachedPage),
                            indexPageManager,
                            treeLeafNodePageManager);
                    treeKeyNodePageManager.bplusTreeRuntimeParameters.setRootPageId(newRootKeyNodePage.pageId);
                    treeKeyNodePageManager.bplusTreeRuntimeParameters.incTreeHeight();
//                    printThread("TreeHeight: Add write Key (Added via most leaf and adding root since mostLeaf is full): " + keyDataTuple.key + " increased tree height to: "
//                            + treeNodePageManager.treeHeader.getTreeHeight(), true);
                    treeKeyNodePageManager.unlockPage(newRootKeyNodePage);
                    printLeaf = true;
                }
                treeLeafNodePageManager.unlockPage(mostLeafCachedPage);
//                System.out.println(mostLeafNode.leafKeys.toString1());
            } finally {

                rootLock.writeLock().unlock();
            }

        }
        if (printLeaf) {
//            printTree();
            printLeaf = false;
        }
        assert rootLock.writeLock().getHoldCount() == 0;
//        for (Map.Entry<PageKey, CachedPageNew<? extends Page>> pageKeyCachedPageNewEntry : myBuffer.lruCache.entrySet()) {
//            CachedPageNew<? extends Page> cachedPage = pageKeyCachedPageNewEntry.getValue();
//            if (cachedPage.pageLock.isWriteLocked()) {
//                printTree();
//                throw new RuntimeException(pageKeyCachedPageNewEntry.getKey().toString());
//            }
//        }
    }

    public static boolean printLeaf = false;

    public void deleteWithWriteLock(String key) throws IOException {
        rootLock.writeLock().lock();
//        printThread("Delete Write Key: " + key , true);
        long rootPageId = treeKeyNodePageManager.bplusTreeRuntimeParameters.getRootPageId();

        if (rootPageId > ZERO_PAGE) { // root exist

            final CachedPageNew<TreeKeyNode> rootPage = treeKeyNodePageManager.getAndLockWriteKeyNodePage(rootPageId);

            TraverseNodes traverseNodes = BPlusKVTableDeleteUtils.traverseDownWrite(
                    rootPage,
                    key,
                    indexPageManager,
                    treeKeyNodePageManager,
                    treeLeafNodePageManager,
                    rootLock);
            if (traverseNodes != null) {
                BPlusKVTableDeleteUtils.deleteFromLeafAndTraverseUp(
                        key,
                        indexPageManager,
                        treeKeyNodePageManager,
                        treeLeafNodePageManager,
                        dataPageManager,
                        traverseNodes,
                        rootLock);
            }

        } else {
            long mostLeftLeafPageId = treeKeyNodePageManager.bplusTreeRuntimeParameters.getMostLeftLeafPageId();
            if (mostLeftLeafPageId > ZERO_PAGE) {
                CachedPageNew<TreeLeafNode> mostLeafCachedPage = treeLeafNodePageManager.getAndLockWriteLeafNodePage(mostLeftLeafPageId);
                TreeLeafNode mostLeafNode = mostLeafCachedPage.page;
                BPlusKVTableDeleteUtils.deleteFromLeaf(mostLeafNode, key, indexPageManager, dataPageManager);
                if (mostLeafNode.leafKeys.isEmpty()) {
                    treeKeyNodePageManager.bplusTreeRuntimeParameters.decTreeHeight();
//                    printThread("TreeHeight: Delete write Key (Most leaf is empty): " + key + " decreased tree height to: "
//                            + treeNodePageManager.treeHeader.getTreeHeight(), true);
                    treeKeyNodePageManager.bplusTreeRuntimeParameters.setMostLeftLeafPageId(-1);
                }
                treeLeafNodePageManager.unlockPage(mostLeafCachedPage);
            }
            rootLock.writeLock().unlock();
        }

        assert rootLock.writeLock().getHoldCount() == 0;

    }

    public KVRecord get(String key) throws IOException {

        rootLock.readLock().lock();
//        printThread("Get : Key: " + key, true);
        try {
            if (treeKeyNodePageManager.bplusTreeRuntimeParameters.getMostLeftLeafPageId() <= ZERO_PAGE) {
                printThread("Get : Key: " + key + "  tree doesn't exist ");
                return null;
            }
            long leafPageId = 0;
            if (treeKeyNodePageManager.bplusTreeRuntimeParameters.rootExist()) {
//                printThread("Get : Key: " + key + "  from root " + treeNodePageManager.treeHeader.getRootPageId(), true);
//                printThread("From root: " + treeNodePageManager.treeHeader.getRootPageId() + " Key: " + key , true);
                leafPageId = BPlusKVTableUtils.getLeafPageId(
                        key,
                        treeKeyNodePageManager.bplusTreeRuntimeParameters.getRootPageId(),
                        0,
                        treeKeyNodePageManager,
                        indexPageManager);
            } else {
//                printThread("Get : Key: " + key + "  from leaf " + treeNodePageManager.treeHeader.getMostLeftLeafPageId(), true);
//                return BPlusKVTableUtils.recurFind(key,
//                        treeNodePageManager.bplusTreeRuntimeParameters.getMostLeftLeafPageId(),
//                        0,
//                        treeNodePageManager,
//                        indexPageManager,
//                        dataPageManager);
               leafPageId = treeKeyNodePageManager.bplusTreeRuntimeParameters.getMostLeftLeafPageId();
            }

//            assert (leafPageId > 0 && leafPageId < 1000);
            return BPlusKVTableUtils.getRecordFromLeaf(
                    leafPageId,
                    indexPageManager,
                    treeLeafNodePageManager,
                    dataPageManager,
                    key);
        } finally {
            rootLock.readLock().unlock();
        }

    }

    public ArrayList<KVRecord> getAll() throws IOException {
        rootLock.readLock().lock();
        ArrayList<KVRecord> list = new ArrayList<>();
        long mostLeftLeafPageId = treeKeyNodePageManager.bplusTreeRuntimeParameters.getMostLeftLeafPageId();
        if (mostLeftLeafPageId > ZERO_PAGE) {
            CachedPageNew<TreeLeafNode> cachedPage = treeLeafNodePageManager.getAndLockReadLeafNodePage(mostLeftLeafPageId);
            if (treeKeyNodePageManager.bplusTreeRuntimeParameters.getMostLeftLeafPageId() == mostLeftLeafPageId) {
                do {
                    if (cachedPage == null) cachedPage = treeLeafNodePageManager.getAndLockReadLeafNodePage(mostLeftLeafPageId);
                    TreeLeafNode leafNode = cachedPage.getPage();
                    mostLeftLeafPageId = leafNode.getRightSibling();
                    for (int i = 0; i < leafNode.leafKeys.getNextKeyNum(); i++) {
                        KeyToDataLocationsItem indexToDataLocation = indexPageManager.getKeyToDataLocation(leafNode.leafKeys.getKeyLocation(i));
                        KVRecord keyDataTuple = dataPageManager.getKeyDataTuple(indexToDataLocation);
                        list.add(keyDataTuple);
                    }
                    treeLeafNodePageManager.unlockPage(cachedPage);
                    cachedPage = null;
                } while (mostLeftLeafPageId != ZERO_PAGE);
            } else {
                rootLock.readLock().unlock();
                return getAll();
            }
        }
        rootLock.readLock().unlock();
        return list;
    }

    public void forceFlush() throws IOException {
        rootLock.writeLock().lock();
        try {
            myBuffer.flushAllForce();
        } finally {
            rootLock.writeLock().unlock();
        }
    }

    // TODO
    @Override
    public void optimize() throws IOException {

    }

    @Override
    public Params<?> getTableProperties() {
        return bplusConf;
    }

    @Override
    public long getRecordsCnt(Query query) {
        throw new NotImplementedException();
    }

    @Override
    public QueryIterator getRangeRecords(Query query) throws IOException {
        return null;
    }

    public void printTree() throws IOException {
        StringBuilder sb = new StringBuilder();
//        sb.append("Thread[" + Thread.currentThread().getId() + "]\n");
        int treeHeight = treeKeyNodePageManager.bplusTreeRuntimeParameters.getTreeHeight();
        int level = 1;
        long mostLeftKeyPageId = treeKeyNodePageManager.bplusTreeRuntimeParameters.getRootPageId();
        while (level < treeHeight) {
            level++;
            CachedPageNew<TreeKeyNode> page = treeKeyNodePageManager.getAndLockReadKeyNodePage(mostLeftKeyPageId);
            mostLeftKeyPageId = page.getPage().keysAndChildren.getLeftChild(0);
            treeKeyNodePageManager.unlockPage(page);
            printKey(page.pageId, sb);
        }
        if (mostLeftKeyPageId == -1) mostLeftKeyPageId = treeKeyNodePageManager.bplusTreeRuntimeParameters.getMostLeftLeafPageId();
        if (mostLeftKeyPageId == -1) {
            System.out.println("null");
        } else {
            printLeaf(mostLeftKeyPageId, sb);
            System.out.println(sb);
        }

    }

    public void printKey(long pageId, StringBuilder sb) throws IOException {
        long curPageId = pageId;
        CachedPageNew<TreeKeyNode> page;
        TreeKeyNode current;
        sb.append("KeyNodes: ");
        do {
            page = treeKeyNodePageManager.getAndLockReadKeyNodePage(curPageId);
            current = page.getPage();
            sb.append(current.toString1(indexPageManager) + "    ");
            curPageId = current.getRightSibling();
            treeKeyNodePageManager.unlockPage(page);
        } while (curPageId != 0);
        sb.append("\n");
    }
    private void printLeaf(long pageId, StringBuilder sb) throws IOException {
        long curPageId = pageId;
        CachedPageNew<TreeLeafNode> page;
        TreeLeafNode current;
        sb.append("LeafNode: ");
        do {
            page = treeLeafNodePageManager.getAndLockReadLeafNodePage(curPageId);
            current = page.getPage();
            sb.append(current.toString1(indexPageManager)).append("    ");
            curPageId = current.getRightSibling();
            treeLeafNodePageManager.unlockPage(page);
        } while (current.getRightSibling() != 0);
        sb.append("\n");
    }

//
//    private void printTree(long pageId, StringBuilder sb) throws IOException {
//        CachedPageNew<TreeNodePage> page = treeNodePageManager.getAndLockReadPage(pageId);
//        if (page.getPage().isLeafNode()) {
//            treeNodePageManager.unlockPage(page);
//            printLeaf(page.pageId, sb);
//        } else {
//            treeNodePageManager.unlockPage(page);
//            printKey(pageId, sb);
//            long mostLeftPageId = TreeKeyNode.apply(page.getPage(), treeNodePageManager.bPlusEngineConf.btreeConf).keysAndChildren.getLeftChild(0);
//
//            printTree(mostLeftPageId, sb);
//        }
//    }
//
//    private void printLeaf(long pageId, StringBuilder sb) throws IOException {
//
//        long curPageId = pageId;
//        CachedPageNew<TreeNodePage> page;
//        TreeLeafNode current;
//        sb.append("LeafNode: ");
//        do {
//            page = treeNodePageManager.getAndLockReadPage(curPageId);
//            current = TreeLeafNode.apply(page.getPage(), treeNodePageManager.bPlusEngineConf.btreeConf);
//            sb.append(current.toString1(indexPageManager)).append("    ");
//            curPageId = current.getRightSibling();
//            treeNodePageManager.unlockPage(page);
//        } while (current.getRightSibling() != 0);
//        sb.append("\n");
//    }
//
//    private void printKey(long pageId, StringBuilder sb) throws IOException {
//        long curPageId = pageId;
//        CachedPageNew<TreeNodePage> page;
//        TreeKeyNode current;
//        sb.append("KeyNodes: ");
//        do {
//            page = treeNodePageManager.getAndLockReadPage(curPageId);
//            current = TreeKeyNode.apply(page.getPage(), treeNodePageManager.bPlusEngineConf.btreeConf);
//            sb.append(current.toString1(indexPageManager) + "    ");
//            curPageId = current.getRightSibling();
//            treeNodePageManager.unlockPage(page);
//        } while (curPageId != 0);
//        sb.append("\n");
//    }

    public void printFreeSpaceInfo() {
        System.out.println("TreeNodePageManager" + treeKeyNodePageManager.printFreeSpaceInfo());
        System.out.println("IndexPageManager" + indexPageManager.printFreeSpaceInfo());
        System.out.println("DataPageManager" + dataPageManager.printFreeSpaceInfo());
    }

    public void printFreeBitsSetTreeNodeManager() {
        System.out.println("BitsSet: " + treeKeyNodePageManager.printSpecTreeNodeChecker());
    }

    public void close() throws IOException {
        treeKeyNodePageManager.close();
        dataPageManager.close();
        indexPageManager.close();
    }



    public void bulkInsert() {

    }

    public static void printThread(String msg) {
        System.out.println("Thread[" + Thread.currentThread().getId() + "]  BPlusKVTable: " + msg);
    }

}
