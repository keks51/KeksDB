package com.keks.kv_storage.bplus.tree.utils;

import com.keks.kv_storage.bplus.buffer.CachedPageNew;
import com.keks.kv_storage.record.KVRecord;
import com.keks.kv_storage.bplus.item.KeyItem;
import com.keks.kv_storage.bplus.page_manager.managers.DataPageManager;
import com.keks.kv_storage.bplus.page_manager.managers.IndexPageManager;
import com.keks.kv_storage.bplus.page_manager.managers.TreeKeyNodePageManager;
import com.keks.kv_storage.bplus.page_manager.managers.TreeLeafNodePageManager;
import com.keks.kv_storage.bplus.tree.KeyToDataLocationsItem;
import com.keks.kv_storage.bplus.tree.node.TreeNode;
import com.keks.kv_storage.bplus.tree.node.key.KeyLocation;
import com.keks.kv_storage.bplus.tree.node.key.TreeKeyNode;
import com.keks.kv_storage.bplus.tree.node.leaf.LeafDataLocation;
import com.keks.kv_storage.bplus.tree.node.leaf.TreeLeafNode;
import com.keks.kv_storage.bplus.tree.node.traverse.TraverseKeyNode;
import com.keks.kv_storage.bplus.tree.node.traverse.TraverseLeafNode;
import com.keks.kv_storage.bplus.tree.node.traverse.TraverseNodes;

import java.io.IOException;
import java.util.Stack;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;


public class BPlusKVTableInsertUtils {

    public static void splitLeaf(boolean isNewParent,
                                 TreeKeyNode parentKeyNode,
                                 TraverseLeafNode leafItem,
                                 IndexPageManager indexPageManager,
                                 TreeLeafNodePageManager treeLeafNodePageManager) throws IOException {
        CachedPageNew<TreeLeafNode> newRightLeafPage = treeLeafNodePageManager.getAndLockWriteFreeLeafNodePage();
        TreeLeafNode newRightLeafNode = newRightLeafPage.page;
        // setting right sibling if left leaf contains
        if (leafItem.getRightSiblingPage() != null) {
            TreeLeafNode rightSibling = leafItem.getRightSiblingPage().getPage();
            rightSibling.setLeftSibling(newRightLeafNode.pageId);
            newRightLeafNode.setRightSibling(rightSibling.pageId);
        }

        TreeLeafNode leftLeafNode = leafItem.nodePage.getPage();

        // moving keys
        leftLeafNode.leafKeys.moveRightHalfToNewRightNode(newRightLeafNode.leafKeys);
        leftLeafNode.keysCache.clear();
        // setting siblings and parent pageId
        newRightLeafNode.setLeftSibling(leftLeafNode.pageId);
        leftLeafNode.setRightSibling(newRightLeafNode.pageId);
        newRightLeafNode.setParent(parentKeyNode.pageId);

        // updating parent
        LeafDataLocation keyDataLocation = leftLeafNode.leafKeys.getMidKeyDataLocation();
        String key = indexPageManager.getKeyToDataLocation(keyDataLocation).key;
        KeyLocation keyLocation = indexPageManager.addIndexedKey(new KeyItem(key));

        parentKeyNode.keysCache.put(keyLocation, key);
        int insertPos = parentKeyNode.getInsertPos(key, indexPageManager);
        int keyNum = (insertPos + 1) * -1;
        if (isNewParent) {
            leftLeafNode.setParent(parentKeyNode.pageId);
            parentKeyNode.keysAndChildren.insertKeyAndChildrenWhenEmpty(keyLocation, leftLeafNode.pageId, newRightLeafNode.pageId);
        } else {
            parentKeyNode.keysAndChildren.insertKeyAndRightChild(keyNum, keyLocation, newRightLeafNode.pageId);
        }

//        printThread("Splitting leaf", true);
//        printThread("Splitting leaf. ParentIsNew: " + isNewParent + "\n"
//                + " Before split:" + "\n"
//                + " Parent: " + beforeSplitParent1 + "     " + beforeSplitParent2 + "\n"
//                + " Left: " + beforeSplitLeft1 + "     " + beforeSplitLeft2 + "\n"
//                + " After split:" + "\n"
//                + " Parent: " + afterSplitParent1 + "     " + afterSplitParent2 + "\n"
//                + " Left: " + afterSplitLeft1 + "     " + afterSplitLeft2 + "\n"
//                + " Right: " + afterSplitRight1 + "     " + afterSplitRight2,true);
        treeLeafNodePageManager.unlockPage(newRightLeafPage);
    }

    private static void splitKeyNodeAndUpdateParent(IndexPageManager indexPageManager,
                                                    TreeKeyNodePageManager treeKeyNodePageManager,
                                                    TreeLeafNodePageManager treeLeafNodePageManager,
                                                    TreeKeyNode parentKeyNode,
                                                    TraverseKeyNode keyNodeItem) throws IOException {
        CachedPageNew<TreeKeyNode> newRightKeyNodePage = treeKeyNodePageManager.getAndLockWriteFreeKeyNodePage();
        TreeKeyNode newRightKeyNode = newRightKeyNodePage.getPage();
        if (keyNodeItem.getRightSiblingPage() != null) { // setting right sibling if left leaf contains
            TreeKeyNode rightSibling = keyNodeItem.getRightSiblingPage().getPage();
            rightSibling.setLeftSibling(newRightKeyNode.pageId);
            newRightKeyNode.setRightSibling(rightSibling.pageId);
        }
        BPlusKVTableInsertUtils.splitKeyNode(
                false,
                parentKeyNode,
                keyNodeItem.nodePage.getPage(),
                newRightKeyNode,
                indexPageManager);

        if (keyNodeItem.childIsLeaf) {
            { // updating most left child of first key
                long mostLeftChild = newRightKeyNode.keysAndChildren.getLeftChild(0);
                CachedPageNew<TreeLeafNode> newRightChildPage = treeLeafNodePageManager.getAndLockWriteLeafNodePage(mostLeftChild);
                TreeNode.setParent(newRightChildPage.getPage(), newRightKeyNode.pageId);
                treeLeafNodePageManager.unlockPage(newRightChildPage);
            }

            for (int i = 0; i < newRightKeyNode.keysAndChildren.getNextKeyNum(); i++) {
                CachedPageNew<TreeLeafNode> newRightChildPage = treeLeafNodePageManager.getAndLockWriteLeafNodePage(newRightKeyNode.keysAndChildren.getRightChild(i));
                TreeNode.setParent(newRightChildPage.getPage(), newRightKeyNode.pageId);
                treeLeafNodePageManager.unlockPage(newRightChildPage);
            }
        } else {
            { // updating most left child of first key
                long mostLeftChild = newRightKeyNode.keysAndChildren.getLeftChild(0);
                CachedPageNew<TreeKeyNode> newRightChildPage = treeKeyNodePageManager.getAndLockWriteKeyNodePage(mostLeftChild);
                TreeNode.setParent(newRightChildPage.getPage(), newRightKeyNode.pageId);
                treeKeyNodePageManager.unlockPage(newRightChildPage);
            }

            for (int i = 0; i < newRightKeyNode.keysAndChildren.getNextKeyNum(); i++) {
                CachedPageNew<TreeKeyNode> newRightChildPage = treeKeyNodePageManager.getAndLockWriteKeyNodePage(newRightKeyNode.keysAndChildren.getRightChild(i));
                TreeNode.setParent(newRightChildPage.getPage(), newRightKeyNode.pageId);
                treeKeyNodePageManager.unlockPage(newRightChildPage);
            }
        }


        treeKeyNodePageManager.unlockPage(newRightKeyNodePage);
    }

    private static void splitKeyNode(boolean isNewParent,
                                     TreeKeyNode parentKeyNode,
                                     TreeKeyNode leftKeyNode,
                                     TreeKeyNode newRightKeyNode,
                                     IndexPageManager indexPageManager) throws IOException {
        // moving keys
        leftKeyNode.keysAndChildren.moveRightHalfToNewRightNode(newRightKeyNode.keysAndChildren);
        leftKeyNode.keysCache.clear();

        // setting siblings
        newRightKeyNode.setLeftSibling(leftKeyNode.pageId);
        leftKeyNode.setRightSibling(newRightKeyNode.pageId);
        newRightKeyNode.setParent(parentKeyNode.pageId);

        KeyLocation midIndex = leftKeyNode.keysAndChildren.getMidIndex();


        if (isNewParent) {
            leftKeyNode.setParent(parentKeyNode.pageId);
            newRightKeyNode.setParent(parentKeyNode.pageId);
            parentKeyNode.keysAndChildren.insertKeyAndChildrenWhenEmpty(midIndex, leftKeyNode.pageId, newRightKeyNode.pageId);
        } else {
            String newParentKey = indexPageManager.getKey(midIndex);
            int insertPos = parentKeyNode.getInsertPos(newParentKey, indexPageManager);
            int keyNum = (insertPos + 1) * -1;
            parentKeyNode.keysAndChildren.insertKeyAndRightChild(keyNum, midIndex, newRightKeyNode.pageId);
            newRightKeyNode.setParent(parentKeyNode.pageId);
        }
    }

    private static AtomicInteger cnt123 = new AtomicInteger();


    public static boolean addToLeaf(TreeLeafNode leafNode,
                                    KVRecord newKeyDataTuple,
                                    IndexPageManager indexPageManager,
                                    DataPageManager dataPageManager) throws IOException {
//        int x = cnt123.get();
//        if (x % 10_000 == 0) {
//            System.out.println(x / 10_000);
//        }
        int insertPos;
        try {
            insertPos = leafNode.getInsertPos(newKeyDataTuple.key, indexPageManager);
        } catch (Throwable t) {
            printThreadOld("Cannot find insert pos for key: "
                    + newKeyDataTuple.key + " in Leaf Page: "
                    + leafNode.pageId
                    + " keys: "
                    + leafNode.leafKeys.toString1());
            throw t;
        }
        if (insertPos < 0) { // insert new
            cnt123.incrementAndGet();

            LeafDataLocation[] dataLocations = dataPageManager.addNewKeyDataTuple(newKeyDataTuple);
            LeafDataLocation keyDataTupleLoc = indexPageManager.addLeafDataLocations(new KeyToDataLocationsItem(newKeyDataTuple.key, newKeyDataTuple.getLen(), dataLocations));
            int keyNum = (insertPos + 1) * -1;
            leafNode.leafKeys.insertNewKey(keyNum, keyDataTupleLoc);
            leafNode.keysCache.put(keyDataTupleLoc, newKeyDataTuple.key);
//            System.out.println("Added new key: " + newKeyDataTuple + " to "  + keyDataTupleLoc);
//            printThread(" Added new key: " + keyDataTupleLoc);
            return false;
        } else { // replace previous
            LeafDataLocation previousLeafDataLocation = leafNode.leafKeys.getKeyLocation(insertPos);
            KeyToDataLocationsItem previousKeyToDataLocations = indexPageManager.getKeyToDataLocation(previousLeafDataLocation);
            LeafDataLocation[] dataLocations = dataPageManager.replaceData(previousKeyToDataLocations.dataLocations, newKeyDataTuple);
            KeyToDataLocationsItem newKeyToData = new KeyToDataLocationsItem(previousKeyToDataLocations.key, newKeyDataTuple.getLen(), dataLocations);
            LeafDataLocation newKeyToDataLocation =
                    indexPageManager.updateKeyToDataLocations(previousLeafDataLocation, previousKeyToDataLocations.getLen(), newKeyToData);
            leafNode.leafKeys.replaceWithNewKey(insertPos, newKeyToDataLocation);
            leafNode.keysCache.put(newKeyToDataLocation, newKeyDataTuple.key);
            return true;
        }
    }

    public static void printThread(String msg) {
        System.out.println("Thread[" + Thread.currentThread().getId() + "]  Buffer: " + msg);
    }


    public static TraverseNodes traverseDownWrite(CachedPageNew<TreeKeyNode> rootPage,
                                                  String key,
                                                  IndexPageManager indexPageManager,
                                                  TreeKeyNodePageManager treeKeyNodePageManager,
                                                  TreeLeafNodePageManager treeLeafNodePageManager,
                                                  ReentrantReadWriteLock rootLock) throws IOException {
        Stack<TraverseKeyNode> lockedCachedPages = new Stack<>();
        TraverseLeafNode traverseLeafNode = null;

        CachedPageNew<TreeKeyNode> parentKeyPage = rootPage;

        int treeHeight = treeKeyNodePageManager.bplusTreeRuntimeParameters.getTreeHeight();
        int level = 1;
        lockedCachedPages.push(new TraverseKeyNode(rootPage, level + 1 == treeHeight));
        while (level < treeHeight) {
            level++;
            long childPageId = parentKeyPage.page.getChildPageId(key, indexPageManager);
            if (level == treeHeight) {
                CachedPageNew<TreeLeafNode> childLeafPage = treeLeafNodePageManager.getAndLockWriteLeafNodePage(childPageId);
                TreeLeafNode childLeafNode = childLeafPage.page;
                int insertPos = childLeafNode.getInsertPos(key, indexPageManager);
                if (insertPos < 0 && childLeafNode.leafKeys.isAlmostFull()) { // if insertPos >= 0 then updating. no need to keep parent nodes lock
//                    lockedCachedPages.push(BPlusKVTableUtils.getNodeTraverseItem(childKeyPage, leafNode, treeNodePageManager));
                    traverseLeafNode = BPlusKVTableUtils.getLeafNodeTraverseItem(childLeafPage, childLeafNode, treeLeafNodePageManager);
                } else {
                    for (TraverseKeyNode e : lockedCachedPages) {
                        if (e.nodePage.pageId == treeKeyNodePageManager.bplusTreeRuntimeParameters.getRootPageId()) {
                            rootLock.writeLock().unlock();
                        }
                        treeKeyNodePageManager.unlockTraverseItem(e);
                    }
                    lockedCachedPages.clear();
                    traverseLeafNode = new TraverseLeafNode(childLeafPage);
                }

            } else { // TODO only parent key node can be locked
                CachedPageNew<TreeKeyNode> childKeyPage = treeKeyNodePageManager.getAndLockWriteKeyNodePage(childPageId);
                TraverseKeyNode childTraverseItem = BPlusKVTableUtils
                        .getKeyNodeTraverseItem(childKeyPage, treeKeyNodePageManager, level + 1 == treeHeight);
//                lockedCachedPages.add(childTraverseItem);
                if (childKeyPage.page.keysAndChildren.isAlmostFull()) {

                } else {
                    for (TraverseKeyNode e : lockedCachedPages) {
                        if (e.nodePage.pageId == treeKeyNodePageManager.bplusTreeRuntimeParameters.getRootPageId()) {
                            rootLock.writeLock().unlock();
                        }
                        treeKeyNodePageManager.unlockTraverseItem(e);
                    }
                    lockedCachedPages.clear();
//                    TraverseKeyNode childTraverseItem = new TraverseKeyNode(childKeyPage, level + 1 == treeHeight);

                }
                lockedCachedPages.add(childTraverseItem);
                parentKeyPage = childKeyPage;
            }

        }


        return new TraverseNodes(lockedCachedPages, traverseLeafNode);
    }

    public static void addToLeafAndTraverseUp(KVRecord keyDataTuple,
                                              IndexPageManager indexPageManager,
                                              TreeKeyNodePageManager treeKeyNodePageManager,
                                              TreeLeafNodePageManager treeLeafNodePageManager,
                                              DataPageManager dataPageManager,
                                              TraverseNodes traverseNodes,
                                              ReentrantReadWriteLock rootLock) throws IOException {
        TraverseLeafNode leafItem = traverseNodes.leafNode;
        Stack<TraverseKeyNode> traverseItems = traverseNodes.keyNodesStack;
        TreeLeafNode leafNode = leafItem.nodePage.getPage();
        boolean wasUpdated = BPlusKVTableInsertUtils.addToLeaf(leafNode, keyDataTuple, indexPageManager, dataPageManager);


        // splitting leaf
        if (leafNode.leafKeys.isFull() && !wasUpdated) {
            TreeKeyNode leafParentKeyNode = traverseItems.peek().nodePage.page;
            splitLeaf(false, leafParentKeyNode, leafItem, indexPageManager, treeLeafNodePageManager);

            while (!traverseItems.isEmpty()) {
                TraverseKeyNode keyNodeItem = traverseItems.pop();
                CachedPageNew<TreeKeyNode> keyNodePage = keyNodeItem.nodePage;
                boolean childIsLeaf = keyNodeItem.childIsLeaf;
                TreeKeyNode keyNode = keyNodePage.getPage();
                if (keyNode.keysAndChildren.isFull()) { // since we lock parent also for updating, parent still can be not full after updating
                    if (keyNode.pageId == treeKeyNodePageManager.bplusTreeRuntimeParameters.getRootPageId()) {
                        addNewRoot(indexPageManager, treeKeyNodePageManager, treeLeafNodePageManager, keyNode, childIsLeaf);
//                        printThreadOld("TreeHeight: Add write Key (Added via traverse up): " + keyDataTuple.key + " increased tree height to: "
//                                + treeKeyNodePageManager.bplusTreeRuntimeParameters.getTreeHeight());
                        rootLock.writeLock().unlock();
                    } else {
                        TreeKeyNode parentKeyNode = traverseItems.peek().nodePage.getPage();
                        splitKeyNodeAndUpdateParent(indexPageManager, treeKeyNodePageManager, treeLeafNodePageManager, parentKeyNode, keyNodeItem);
                    }
                }
                if (keyNode.pageId == treeKeyNodePageManager.bplusTreeRuntimeParameters.getRootPageId()) {
                    rootLock.writeLock().unlock();
                }
                treeKeyNodePageManager.unlockTraverseItem(keyNodeItem);
            }
        }
        assert traverseItems.isEmpty();
        treeLeafNodePageManager.unlockTraverseItem(leafItem);
    }


    private static void addNewRoot(IndexPageManager indexPageManager,
                                   TreeKeyNodePageManager treeKeyNodePageManager,
                                   TreeLeafNodePageManager treeLeafNodePageManager,
                                   TreeKeyNode oldRootKeyNode,
                                   boolean childIsLeaf) throws IOException {

        CachedPageNew<TreeKeyNode> newRootPage = treeKeyNodePageManager.getAndLockWriteFreeKeyNodePage();
        CachedPageNew<TreeKeyNode> newRightPage = treeKeyNodePageManager.getAndLockWriteFreeKeyNodePage();
        TreeKeyNode newRightKeyNode = newRightPage.getPage();
//        printThread("New Root2: " + newRootPage.pageId + " LeftChild: "
//                + oldRootKeyNode.pageId + " RightChild: "
//                + newRightPage.pageId, true);
        BPlusKVTableInsertUtils.splitKeyNode(true,
                newRootPage.getPage(),
                oldRootKeyNode,
                newRightKeyNode,
                indexPageManager);
        if (childIsLeaf) {
            // maybe we don't need a parent at all
            { // updating most left child of first key
                long mostLeftChild = newRightKeyNode.keysAndChildren.getLeftChild(0);
                CachedPageNew<TreeLeafNode> newRightChildPage = treeLeafNodePageManager.getAndLockWriteLeafNodePage(mostLeftChild);
                TreeNode.setParent(newRightChildPage.getPage(), newRightKeyNode.pageId);
                treeLeafNodePageManager.unlockPage(newRightChildPage);
            }

            // updating right child of each key
            for (int i = 0; i < newRightKeyNode.keysAndChildren.getNextKeyNum(); i++) {
                long rightChild = newRightKeyNode.keysAndChildren.getRightChild(i);
                CachedPageNew<TreeLeafNode> newRightChildPage = treeLeafNodePageManager.getAndLockWriteLeafNodePage(rightChild);
                TreeNode.setParent(newRightChildPage.getPage(), newRightKeyNode.pageId);
                treeLeafNodePageManager.unlockPage(newRightChildPage);

            }
        } else {
            // maybe we don't need a parent at all
            { // updating most left child of first key
                long mostLeftChild = newRightKeyNode.keysAndChildren.getLeftChild(0);
                CachedPageNew<TreeKeyNode> newRightChildPage = treeKeyNodePageManager.getAndLockWriteKeyNodePage(mostLeftChild);
                TreeNode.setParent(newRightChildPage.getPage(), newRightKeyNode.pageId);
                treeKeyNodePageManager.unlockPage(newRightChildPage);
            }

            // updating right child of each key
            for (int i = 0; i < newRightKeyNode.keysAndChildren.getNextKeyNum(); i++) {
                long rightChild = newRightKeyNode.keysAndChildren.getRightChild(i);
                CachedPageNew<TreeKeyNode> newRightChildPage = treeKeyNodePageManager.getAndLockWriteKeyNodePage(rightChild);
                TreeNode.setParent(newRightChildPage.getPage(), newRightKeyNode.pageId);
                treeKeyNodePageManager.unlockPage(newRightChildPage);

            }
        }
        treeKeyNodePageManager.bplusTreeRuntimeParameters.setRootPageId(newRootPage.pageId);
//        printThread("Set successfully new root: " + newRootPage.pageId
//                + " since prev root: " + oldRootKeyNode.pageId  + " is full", true);
        treeKeyNodePageManager.bplusTreeRuntimeParameters.incTreeHeight();
        treeKeyNodePageManager.unlockPage(newRootPage);
        treeKeyNodePageManager.unlockPage(newRightPage);

    }


//    public static class BulkInsert {
//
//        static class ChildNodeKey {
//
//            private final String key;
//            private final long childPageId;
//
//            public ChildNodeKey(String key, long childPageId) {
//                this.key = key;
//                this.childPageId = childPageId;
//            }
//
//        }
//
//        private final BtreeConf btreeConf;
//        private final DataPageManager dataPageManager;
//        private final IndexPageManager indexPageManager;
//        private final TreeNodePageManager treeNodePageManager;
//
//        private final int maxKeysInNode;
//        private final double MAX_LOAD_FACTOR = 0.75;
//
//        private int leafNodesCnt = 0;
//        private int levelNodesCnt = 0;
//        private int recordsCnt = 0;
//        private long firstChildPageId = 1;
//
//        private CachedPageNew<TreeNodePage> leftLeafPage;
//        private TreeLeafNode leftLeafNode;
//
//        private CachedPageNew<TreeNodePage> leftNodePage;
//        private TreeKeyNode leftKeyNode;
//
//        private Queue<LeafDataLocation> leafRecords = new LinkedList<>();
//        private Queue<ChildNodeKey> children = new LinkedList<>();
//
//
//        public BulkInsert(BtreeConf btreeConf,
//                          DataPageManager dataPageManager,
//                          IndexPageManager indexPageManager,
//                          TreeNodePageManager treeNodePageManager) throws IOException {
//            this.btreeConf = btreeConf;
//            this.dataPageManager = dataPageManager;
//            this.indexPageManager = indexPageManager;
//            this.treeNodePageManager = treeNodePageManager;
//            this.maxKeysInNode = (int) (btreeConf.maxKeys * MAX_LOAD_FACTOR);
//            this.leftLeafPage = treeNodePageManager.getAndLockWritePage(1);
//            this.leftLeafNode = TreeLeafNode.apply(leftLeafPage.getPage(), treeNodePageManager.bPlusEngineConf.btreeConf);
//        }
//
////        public void insert(KeyDataTuple keyDataTuple) throws IOException {
////            recordsCnt++;
////            if (recordsCnt % maxKeysInNode == 0) {
////                children.add(new ChildNodeKey(keyDataTuple.key, leftLeafNode.pageId));
////            }
////
////            LeafDataLocation[] leafDataLocations = dataPageManager.addNewKeyDataTuple(keyDataTuple);
////            LeafDataLocation leafDataLocation = indexPageManager.addLeafDataLocations(new KeyToDataLocationsItem(keyDataTuple.key, keyDataTuple.getDataLen(), leafDataLocations));
////            leafRecords.add(leafDataLocation);
////
////            if (leafRecords.size() >= maxKeysInNode * 2) {
////                fillLeaf(maxKeysInNode);
////                addNextLeaf();
////            }
////        }
//
////        public void buildTree() throws IOException {
////            if (leafRecords.size() > btreeParameters.maxKeys) {
////                fillLeaf(leafRecords.size() / 2);
////                addNextLeaf();
////            }
////            fillLeaf(leafRecords.size());
////            treeNodePageManager.unlockPage(leftLeafPage);
////
////            if (leafNodesCnt != 1) {
////                leftNodePage = treeNodePageManager.getAndLockWriteFreePage();
////                TreeKeyNode.apply(leftNodePage.getPage());
////                createNodes();
////            }
////        }
//
////        private void createNodes() throws IOException {
////            int keyNodesCnt = 0;
////
////            do {
////                leftKeyNode.keysAndChildren.addRightChildToEnd(firstChildPageId);
////                while (!children.isEmpty()) {
////                    if (children.size() > maxKeysInNode) {
////                        fillKeyNode(maxKeysInNode);
////
////                        addNextKeyNode();
////                    } else {
////                        fillKeyNode(children.size());
////                    }
////                }
////
////
////
////            } while (levelNodesCnt != 1);
////
////            treeNodePageManager.treeHeader.setRootPageId(leftKeyNode.pageId);
////        }
//
//        private void fillKeyNode(int records) throws IOException {
//            for (int i = 0; i < records; i++) {
//                ChildNodeKey childNodeKey = children.remove();
//                String key = childNodeKey.key;
//                long childPageId = childNodeKey.childPageId;
//
//                setParentForChildNode(childPageId, leftNodePage.pageId);
//                KeyLocation keyLocation = indexPageManager.addIndexedKey(new KeyItem(key));
//                leftKeyNode.keysAndChildren.appendKeyAndLeftChild(keyLocation, childPageId);
//            }
//            levelNodesCnt++;
//        }
//
////        private void addNextKeyNode() throws IOException {
////            CachedPageNew<TreeNodePage> nextKeyNodePage = treeNodePageManager.getAndLockWriteFreePage();
////            TreeKeyNode nextKeyNode = TreeKeyNode.apply(nextKeyNodePage.getPage());
////            nextKeyNode.setLeftSibling(leftNodePage.pageId);
////            leftKeyNode.setRightSibling(nextKeyNodePage.pageId);
////            treeNodePageManager.unlockPage(leftNodePage);
////            leftNodePage = nextKeyNodePage;
////            leftKeyNode = nextKeyNode;
////        }
//
//        private void setParentForChildNode(long childPageId, long parentPageId) throws IOException {
//            CachedPageNew<TreeNodePage> childPage = treeNodePageManager.getAndLockWritePage(childPageId);
//            TreeNode.setParent(childPage.getPage(), parentPageId);
//            treeNodePageManager.unlockPage(childPage);
//        }
//
//        private void fillLeaf(int recordsInLeaf) throws IOException {
//            for (int i = 0; i < recordsInLeaf; i++) {
//                LeafDataLocation leafDataLocation = leafRecords.remove();
//                leftLeafNode.leafKeys.insertNewKey(i, leafDataLocation);
//            }
//            leafNodesCnt++;
//        }
//
////        private void addNextLeaf() throws IOException {
////            CachedPageNew<TreeNodePage> nextLeafPage = treeNodePageManager.getAndLockWriteFreePage();
////            TreeLeafNode nextTreeLeafNode = TreeLeafNode.apply(nextLeafPage.getPage());
////            nextTreeLeafNode.setLeftSibling(leftLeafPage.pageId);
////            leftLeafNode.setRightSibling(nextLeafPage.pageId);
////            treeNodePageManager.unlockPage(leftLeafPage);
////            leftLeafPage = nextLeafPage;
////            leftLeafNode = nextTreeLeafNode;
////        }
//
//
//
//    }

    public static void printThreadOld(String msg) {
        System.out.println("Thread[" + Thread.currentThread().getId() + "]  BPlusKVTableInsertUtils: " + msg);
    }

}


























