package com.keks.kv_storage.bplus.tree.utils;

import com.keks.kv_storage.bplus.buffer.CachedPageNew;
import com.keks.kv_storage.bplus.item.KeyItem;
import com.keks.kv_storage.bplus.page_manager.managers.DataPageManager;
import com.keks.kv_storage.bplus.page_manager.managers.IndexPageManager;
import com.keks.kv_storage.bplus.page_manager.managers.TreeKeyNodePageManager;
import com.keks.kv_storage.bplus.page_manager.managers.TreeLeafNodePageManager;
import com.keks.kv_storage.bplus.tree.KeyToDataLocationsItem;
import com.keks.kv_storage.bplus.tree.node.TreeNode;
import com.keks.kv_storage.bplus.tree.node.key.KeyLocation;
import com.keks.kv_storage.bplus.tree.node.key.KeyLocationAndLeftChild;
import com.keks.kv_storage.bplus.tree.node.key.KeyLocationAndRightChild;
import com.keks.kv_storage.bplus.tree.node.key.TreeKeyNode;
import com.keks.kv_storage.bplus.tree.node.leaf.LeafDataLocation;
import com.keks.kv_storage.bplus.tree.node.leaf.TreeLeafNode;
import com.keks.kv_storage.bplus.tree.node.traverse.TraverseKeyNode;
import com.keks.kv_storage.bplus.tree.node.traverse.TraverseLeafNode;
import com.keks.kv_storage.bplus.tree.node.traverse.TraverseNodes;

import java.io.IOException;
import java.util.Stack;
import java.util.concurrent.locks.ReentrantReadWriteLock;


public class BPlusKVTableDeleteUtils {


    public static boolean deleteFromLeaf(TreeLeafNode leafNode,
                                         String key,
                                         IndexPageManager indexPageManager,
                                         DataPageManager dataPageManager) throws IOException {
        int insertPos = leafNode.getInsertPos(key, indexPageManager);
        if (insertPos < 0) { // insert new
            return false;
        } else { // replace previous
//            PageBuffer.printThread(" Deleting key: " + key, true);
            LeafDataLocation leafKeyLocation = leafNode.leafKeys.getKeyLocation(insertPos);
            KeyToDataLocationsItem leafKeyToDataLocations = indexPageManager.getKeyToDataLocation(leafKeyLocation);
            dataPageManager.deleteKeyDataTuple(leafKeyToDataLocations);
            indexPageManager.deleteLeafDataLocation(leafKeyLocation);
            leafNode.leafKeys.delete(insertPos);
            leafNode.keysCache.remove(leafKeyLocation);
//            printThread(" removed key: from page: " + leafNode.pageId + " " + leafKeyLocation + " " + leafNode.leafKeys.toString1());
//            PageBuffer.printThread(" Deleted key: " + key + " From " + leafKeyLocation, true);
            return true;
        }
    }

    public static void printThread(String msg) {
        System.out.println("Thread[" + Thread.currentThread().getId() + "]  Buffer: " + msg);
    }
//    public static CachedPageNew<TreeNodePage> traverseDownRead(CachedPageNew<TreeNodePage> rootPage,
//                                                                     String key,
//                                                                     IndexPageManager indexPageManager,
//                                                                     TreeNodePageManager treeNodePageManager,
//                                                                     int treeHeight) throws IOException {
//        CachedPageNew<TreeNodePage> parentPage = rootPage;
//        CachedPageNew<TreeNodePage> childPage = null;
//        assert treeHeight > 1;
//        int curDepth = 1;
//        while (true) {
//            try {
//                TreeKeyNode parentKeyNode = TreeKeyNode.apply(parentPage.getPage(), treeNodePageManager.bPlusEngineConf.btreeConf);
//                long childPageId = parentKeyNode.getChildPageId(key, indexPageManager);
//
//                if (curDepth == treeHeight - 1) {
//                    childPage = treeNodePageManager.getAndLockWritePage(childPageId);
//                    TreeLeafNode leafNode = TreeLeafNode.apply(childPage.getPage(), treeNodePageManager.bPlusEngineConf.btreeConf);
//                    if (leafNode.leafKeys.isAlmostDeficient()) { // if insertPos >= 0 then updating. no need to keep parent nodes lock
//                        treeNodePageManager.unlockPage(childPage);
//                        return null;
//                    } else {
//                        return childPage;
//                    }
//                } else {
//                    childPage = treeNodePageManager.getAndLockReadPage(childPageId);
//                    TreeKeyNode childKeyNode = TreeKeyNode.apply(childPage.getPage(), treeNodePageManager.bPlusEngineConf.btreeConf);
//                    if (childKeyNode.keysAndChildren.isAlmostDeficient()) {
//                        treeNodePageManager.unlockPage(childPage);
//                        return null;
//                    }
//                }
//                curDepth++;
//            } finally {
//                treeNodePageManager.unlockPage(parentPage);
//                parentPage = childPage;
//            }
//        }
//    }

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
                TreeLeafNode leafNode = childLeafPage.page;
                int insertPos = leafNode.getInsertPos(key, indexPageManager);
                if (insertPos >= 0) {
                    if (leafNode.leafKeys.isAlmostDeficient()) {
                        traverseLeafNode = BPlusKVTableUtils.getLeafNodeTraverseItem(childLeafPage, leafNode, treeLeafNodePageManager);
                    } else {
//                        printThread(" found pos in normal page: " + childPage.key + " ParentPage: " +  parentPage.key + " " + leafNode.leafKeys.toString1());
                        for (TraverseKeyNode e : lockedCachedPages) {
                            if (e.nodePage.pageId == treeKeyNodePageManager.bplusTreeRuntimeParameters.getRootPageId()) {
                                rootLock.writeLock().unlock();
                            }
                            treeKeyNodePageManager.unlockTraverseItem(e);
                        }
                        lockedCachedPages.clear();
                        traverseLeafNode = new TraverseLeafNode(childLeafPage);
                    }
                } else {
                    for (TraverseKeyNode e : lockedCachedPages) {
                        if (e.nodePage.pageId == treeKeyNodePageManager.bplusTreeRuntimeParameters.getRootPageId()) {
                            rootLock.writeLock().unlock();
                        }
                        treeKeyNodePageManager.unlockTraverseItem(e);
                    }
                    treeLeafNodePageManager.unlockPage(childLeafPage);
                    lockedCachedPages.clear();
                    return null;
                }
            } else {
                CachedPageNew<TreeKeyNode> childKeyPage = treeKeyNodePageManager.getAndLockWriteKeyNodePage(childPageId);
                TraverseKeyNode childTraverseItem = BPlusKVTableUtils
                        .getKeyNodeTraverseItem(childKeyPage, treeKeyNodePageManager, level + 1 == treeHeight);

                if (childKeyPage.page.keysAndChildren.isAlmostDeficient()) {

                } else {
                    for (TraverseKeyNode e : lockedCachedPages) {
                        if (e.nodePage.pageId == treeKeyNodePageManager.bplusTreeRuntimeParameters.getRootPageId()) {
                            rootLock.writeLock().unlock();
                        }
                        treeKeyNodePageManager.unlockTraverseItem(e);
                    }
                    lockedCachedPages.clear();
                }
                lockedCachedPages.push(childTraverseItem);
                parentKeyPage = childKeyPage;
            }

        }


        return new TraverseNodes(lockedCachedPages, traverseLeafNode);
    }

    public static void deleteFromLeafAndTraverseUp(String key,
                                                   IndexPageManager indexPageManager,
                                                   TreeKeyNodePageManager treeKeyNodePageManager,
                                                   TreeLeafNodePageManager treeLeafNodePageManager,
                                                   DataPageManager dataPageManager,
                                                   TraverseNodes traverseNodes,
                                                   ReentrantReadWriteLock rootLock) throws IOException {
        TraverseLeafNode leafItem = traverseNodes.leafNode;
        Stack<TraverseKeyNode> traverseItems = traverseNodes.keyNodesStack;
        TreeLeafNode leafNode = leafItem.nodePage.getPage();
        boolean wasDeleted = BPlusKVTableDeleteUtils.deleteFromLeaf(leafNode, key, indexPageManager, dataPageManager);

        // merging leaf
        if (leafNode.leafKeys.isDeficient() && wasDeleted) {
            CachedPageNew<TreeKeyNode> leafParentKeyNodePage = traverseItems.peek().nodePage;
            TreeKeyNode leafParentKeyNode = leafParentKeyNodePage.page;
            handleDeficientLeaf(leafParentKeyNode, leafItem, indexPageManager, treeKeyNodePageManager);


            if (leafParentKeyNode.pageId == treeKeyNodePageManager.bplusTreeRuntimeParameters.getRootPageId()
                    && leafParentKeyNode.keysAndChildren.getNextKeyNum() == 0) {
                treeKeyNodePageManager.bplusTreeRuntimeParameters.setRootPageId(-1);
                treeKeyNodePageManager.bplusTreeRuntimeParameters.decTreeHeight();
//                PageBuffer.printThread("TreeHeight: Delete write Key (Added via traverse up when child is leaf): " + key + " decreased tree height to: "
//                        + treeNodePageManager.treeHeader.getTreeHeight(), true);
                long mostLeftLeafPageId = leafParentKeyNode.keysAndChildren.getLeftChild(0);
                if (treeKeyNodePageManager.bplusTreeRuntimeParameters.getMostLeftLeafPageId() != mostLeftLeafPageId) {
                    treeKeyNodePageManager.bplusTreeRuntimeParameters.setMostLeftLeafPageId(mostLeftLeafPageId);
                }
                treeLeafNodePageManager.unlockTraverseItem(leafItem);
                treeKeyNodePageManager.unlockTraverseItem(traverseItems.pop());
                rootLock.writeLock().unlock();
            } else {
                treeLeafNodePageManager.unlockTraverseItem(leafItem);

                while (!traverseItems.isEmpty()) {
                    TraverseKeyNode keyNodeItem = traverseItems.pop();
                    TreeKeyNode keyNode = keyNodeItem.nodePage.getPage();
                    if (keyNode.pageId == treeKeyNodePageManager.bplusTreeRuntimeParameters.getRootPageId()) {
                        if (keyNode.keysAndChildren.getNextKeyNum() == 0) {
                            treeKeyNodePageManager.bplusTreeRuntimeParameters.decTreeHeight();
//                            PageBuffer.printThread("TreeHeight: Delete write Key (Added via traverse up): " + key + " decreased tree height to: "
//                                    + treeNodePageManager.treeHeader.getTreeHeight(), true);
                            long mostLeftKeyNode = keyNode.keysAndChildren.getLeftChild(0);
                            treeKeyNodePageManager.bplusTreeRuntimeParameters.setRootPageId(mostLeftKeyNode);
                            CachedPageNew<TreeKeyNode> newRootCached = treeKeyNodePageManager.getAndLockWriteKeyNodePage(mostLeftKeyNode);
                            treeKeyNodePageManager.unlockPage(newRootCached);
                        }
                        treeKeyNodePageManager.unlockTraverseItem(keyNodeItem);
                        rootLock.writeLock().unlock();
                    } else {
                        if (keyNode.keysAndChildren.isDeficient()) {
                            TreeKeyNode parentKeyNode = traverseItems.peek().nodePage.getPage();
                            handleDeficientKeyNode(parentKeyNode, keyNodeItem, indexPageManager, treeKeyNodePageManager, treeLeafNodePageManager);
                        }
                        if (keyNode.pageId == treeKeyNodePageManager.bplusTreeRuntimeParameters.getRootPageId()) rootLock.writeLock().unlock();
                        treeKeyNodePageManager.unlockTraverseItem(keyNodeItem);
                    }
                }
            }
        } else {
            treeLeafNodePageManager.unlockTraverseItem(leafItem);
        }

        assert traverseItems.isEmpty();
    }

    private static void handleDeficientKeyNode(TreeKeyNode parentKeyNode,
                                               TraverseKeyNode keyNodeItem,
                                               IndexPageManager indexPageManager,
                                               TreeKeyNodePageManager treeKeyNodePageManager,
                                               TreeLeafNodePageManager treeLeafNodePageManager) throws IOException {
        CachedPageNew<TreeKeyNode> keyNodePage = keyNodeItem.nodePage;
        CachedPageNew<TreeKeyNode> leftSiblingPage = keyNodeItem.getLeftSiblingPage();
        CachedPageNew<TreeKeyNode> rightSiblingPage = keyNodeItem.getRightSiblingPage();

        TreeKeyNode keyNode = keyNodePage.getPage();
        TreeKeyNode leftSiblingNode = null;
        if (leftSiblingPage != null) leftSiblingNode = leftSiblingPage.getPage();
        TreeKeyNode rightSiblingNode = null;
        if (rightSiblingPage != null) rightSiblingNode = rightSiblingPage.getPage();

        if (leftSiblingNode != null
                && leftSiblingNode.getParent() == parentKeyNode.pageId
                && leftSiblingNode.keysAndChildren.isLendable()) {
            updateChildWithNewParent(
                    keyNode.pageId,
                    leftSiblingNode.keysAndChildren.getRightChildOfLastKey(),
                    keyNodeItem.childIsLeaf,
                    treeKeyNodePageManager,
                    treeLeafNodePageManager);
            borrowKeyFromLeftAndReplaceParentKey(parentKeyNode, keyNode, leftSiblingNode, indexPageManager);

        } else if (rightSiblingNode != null
                && rightSiblingNode.getParent() == parentKeyNode.pageId
                && rightSiblingNode.keysAndChildren.isLendable()) {
            updateChildWithNewParent(
                    keyNode.pageId,
                    rightSiblingNode.keysAndChildren.getLeftChildOfFirstKey(),
                    keyNodeItem.childIsLeaf,
                    treeKeyNodePageManager,
                    treeLeafNodePageManager);
            borrowKeyFromRightAndReplaceParentKey(parentKeyNode, keyNode, rightSiblingNode, indexPageManager);

        } else if (leftSiblingNode != null
                && leftSiblingNode.getParent() == parentKeyNode.pageId
                && leftSiblingNode.keysAndChildren.isMergeable()) {
            updateChildrenWithNewParent(
                    keyNode,
                    leftSiblingNode.pageId,
                    keyNodeItem.childIsLeaf,
                    treeKeyNodePageManager,
                    treeLeafNodePageManager);
            moveAllKeyNodeKeysToLeftNode(parentKeyNode, keyNode, leftSiblingNode, rightSiblingNode, indexPageManager);

        } else if (rightSiblingNode != null
                && rightSiblingNode.getParent() == parentKeyNode.pageId
                && rightSiblingNode.keysAndChildren.isMergeable()) {
            updateChildrenWithNewParent(
                    keyNode,
                    rightSiblingNode.pageId,
                    keyNodeItem.childIsLeaf,
                    treeKeyNodePageManager,
                    treeLeafNodePageManager);
            moveAllKeyNodeKeysToRightNode(parentKeyNode, keyNode, leftSiblingNode, rightSiblingNode, indexPageManager);

        } else {
            throw new RuntimeException("here1234545");
        }

    }

    // order = 5, minKeys = 2
    //              3 7 n n
    // 1 2 n n      4 5 n n <-- [8 9 n n]       delete(9)
    //                ||
    //              3 n n n
    // 1 2 n n      4 5 7 8
    private static void moveAllKeyNodeKeysToLeftNode(TreeKeyNode parentNode,
                                                     TreeKeyNode deficientKeyNode,
                                                     TreeKeyNode leftKeyNode,
                                                     TreeKeyNode rightKeyNode,
                                                     IndexPageManager indexPageManager) throws IOException {
        int keyNumOfLeftChild = parentNode.keysAndChildren.getKeyNumOfRightChild(deficientKeyNode.pageId);
        KeyLocation parentKeyLocation = parentNode.keysAndChildren.removeKeyAndRightChild(keyNumOfLeftChild).keyLocation;
        leftKeyNode.setRightSibling(deficientKeyNode.getRightSibling());
        if (rightKeyNode != null) {
            rightKeyNode.setLeftSibling(leftKeyNode.pageId);
        }
        leftKeyNode.keysAndChildren.addKeyToEnd(parentKeyLocation);
        leftKeyNode.keysAndChildren.joinWithRightNode(deficientKeyNode.keysAndChildren);
        deficientKeyNode.setRightSibling(0);
        deficientKeyNode.setLeftSibling(0);
        deficientKeyNode.keysCache.clear();
        assert deficientKeyNode.keysAndChildren.isEmpty();
        assert !deficientKeyNode.keysAndChildren.isFull();
    }

    // order = 5, minKeys = 2
    //                4 8 n n
    // [0 2 n n]      5 6 n n <-- 8 10 n n       delete(2)
    //                ||
    //              8 n n n
    //         0 4 5 6 <-- 8 10 n n
    public static void moveAllKeyNodeKeysToRightNode(TreeKeyNode parentNode,
                                                     TreeKeyNode deficientKeyNode,
                                                     TreeKeyNode leftKeyNode,
                                                     TreeKeyNode rightKeyNode,
                                                     IndexPageManager indexPageManager) throws IOException {
        int keyNumOfLeftChild = parentNode.keysAndChildren.getKeyNumOfLeftChild(deficientKeyNode.pageId);
        KeyLocationAndLeftChild parentKeyLocation = parentNode.keysAndChildren.removeKeyAndLeftChild(keyNumOfLeftChild);
        rightKeyNode.setLeftSibling(deficientKeyNode.getLeftSibling());
        if (leftKeyNode != null) {
            leftKeyNode.setRightSibling(rightKeyNode.pageId);
        }
        rightKeyNode.keysAndChildren.insertKeyToHead(parentKeyLocation.keyLocation);
        rightKeyNode.keysAndChildren.joinWithLeftNode(deficientKeyNode.keysAndChildren);
        deficientKeyNode.setRightSibling(0);
        deficientKeyNode.setLeftSibling(0);
        deficientKeyNode.keysCache.clear();
        assert deficientKeyNode.keysAndChildren.isEmpty();
        assert !deficientKeyNode.keysAndChildren.isFull();
    }

    //              3 7 n n
    // 1 2 n n      4 5 6 n <-- [8 9 n n]       delete(9)
    //                ||
    //              3 6 n n
    // 1 2 n n      4 5 n n <-- [7 8 n n]
    private static void borrowKeyFromLeftAndReplaceParentKey(TreeKeyNode parentNode,
                                                             TreeKeyNode deficientKeyNode,
                                                             TreeKeyNode leftKeyNode,
                                                             IndexPageManager indexPageManager) {
        int keyNumOfLeftChild = parentNode.keysAndChildren.getKeyNumOfLeftChild(leftKeyNode.pageId);
        KeyLocation parentKeyLocation = parentNode.keysAndChildren.getKeyLocation(keyNumOfLeftChild);
        parentNode.keysCache.remove(parentKeyLocation);

        KeyLocationAndRightChild borrowedKeyLocation = leftKeyNode.keysAndChildren.removeLastKeyAndRightChild();
        leftKeyNode.keysCache.remove(borrowedKeyLocation.keyLocation);

        parentNode.keysAndChildren.replaceWithNewKey(keyNumOfLeftChild, borrowedKeyLocation.keyLocation);

        deficientKeyNode.keysAndChildren.insertToHeadAndLeftChild(parentKeyLocation, borrowedKeyLocation.rightChild);
    }

    // order = 5, minKeys = 2
    //               3 6 n n
    // 1 2 n n      [4 5 n n]  <--  7 8 9 n       delete(5)
    //                ||
    //              3 7 n n
    // 1 2 n n      [4 6 n n]  <--  8 9 n n
    private static void borrowKeyFromRightAndReplaceParentKey(TreeKeyNode parentNode,
                                                              TreeKeyNode deficientKeyNode,
                                                              TreeKeyNode rightKeyNode,
                                                              IndexPageManager indexPageManager) {
        int keyNumOfLeftChild = parentNode.keysAndChildren.getKeyNumOfRightChild(rightKeyNode.pageId);
        KeyLocation parentKeyLocation = parentNode.keysAndChildren.getKeyLocation(keyNumOfLeftChild);
        parentNode.keysCache.remove(parentKeyLocation);

        KeyLocationAndLeftChild borrowedKeyLocation = rightKeyNode.keysAndChildren.removeFirstKeyAndLeftChild();
        rightKeyNode.keysCache.remove(borrowedKeyLocation.keyLocation);
        parentNode.keysAndChildren.replaceWithNewKey(keyNumOfLeftChild, borrowedKeyLocation.keyLocation);
        deficientKeyNode.keysAndChildren.addKeyAndRightChildToEnd(parentKeyLocation, borrowedKeyLocation.leftChild);
    }


    private static void handleDeficientLeaf(TreeKeyNode parentKeyNode,
                                            TraverseLeafNode leafItem,
                                            IndexPageManager indexPageManager,
                                            TreeKeyNodePageManager treeKeyNodePageManager) throws IOException {
        CachedPageNew<TreeLeafNode> leafPage = leafItem.nodePage;
        CachedPageNew<TreeLeafNode> leftSiblingPage = leafItem.getLeftSiblingPage();
        CachedPageNew<TreeLeafNode> rightSiblingPage = leafItem.getRightSiblingPage();

        TreeLeafNode leafNode = leafPage.getPage();
//        long parent = leafNode.getParent();
//        long parent1 = leftSiblingPage.page.getParent();
//        long parent2 = rightSiblingPage.page.getParent();
        TreeLeafNode leftSiblingNode = null;
        if (leftSiblingPage != null) leftSiblingNode = leftSiblingPage.getPage();
        TreeLeafNode rightSiblingNode = null;
        if (rightSiblingPage != null) rightSiblingNode = rightSiblingPage.getPage();

        if (leftSiblingNode != null
                && leftSiblingNode.getParent() == parentKeyNode.pageId
                && leftSiblingNode.leafKeys.isLendable()) {
            borrowKeyFromLeftLeafAndReplaceParentKey(parentKeyNode, leafNode, leftSiblingNode, indexPageManager);

        } else if (rightSiblingNode != null
                && rightSiblingNode.getParent() == parentKeyNode.pageId
                && rightSiblingNode.leafKeys.isLendable()) {
            borrowKeyFromRightLeafAndReplaceParentKey(parentKeyNode, leafNode, rightSiblingNode, indexPageManager);

        } else if (leftSiblingNode != null
                && leftSiblingNode.getParent() == parentKeyNode.pageId
                && leftSiblingNode.leafKeys.isMergeable()) {

            moveAllLeafKeysToLeftLeafNode(parentKeyNode, leafNode, leftSiblingNode, rightSiblingNode, indexPageManager);

        } else if (rightSiblingNode != null
                && rightSiblingNode.getParent() == parentKeyNode.pageId
                && rightSiblingNode.leafKeys.isMergeable()) {
            if (leafNode.pageId == treeKeyNodePageManager.bplusTreeRuntimeParameters.getMostLeftLeafPageId()) {
//                PageBuffer.printThread("Moving old mostLeaf: "
//                        + leafNode.pageId
//                        + " to RightSibling: " + rightSiblingNode.pageId
//                        + " RightSiblingKeys: " + rightSiblingNode.leafKeys.toString1()
//                        + " RightSiblingNextKey: " + rightSiblingNode.leafKeys.getNextKeyNum()
//                        + " RightSibling is free: "
//                        + treeNodePageManager.isPageFree(rightSiblingNode.pageId), true);
                treeKeyNodePageManager.bplusTreeRuntimeParameters.setMostLeftLeafPageId(rightSiblingNode.pageId);
            }
            moveAllLeafKeysToRightLeafNode(parentKeyNode, leafNode, leftSiblingNode, rightSiblingNode, indexPageManager);
//            PageBuffer.printThread(" RightSibling is free: " + treeNodePageManager.isPageFree(rightSiblingNode.pageId), true);
        } else {
            throw new RuntimeException("Cannot handle deficient leaf: " + leafPage.pageId + " min keys: " + treeKeyNodePageManager.bPlusConf.btreeConf.minKeys);
        }


    }

    private static void borrowKeyFromLeftLeafAndReplaceParentKey(TreeKeyNode parentNode,
                                                                 TreeLeafNode deficientLeafNode,
                                                                 TreeLeafNode leftLeafNode,
                                                                 IndexPageManager indexPageManager) throws IOException {
        int keyNumOfLeftChild = parentNode.keysAndChildren.getKeyNumOfLeftChild(leftLeafNode.pageId);
        KeyLocation keyLocationToDelete = parentNode.keysAndChildren.getKeyLocation(keyNumOfLeftChild);
        parentNode.keysCache.remove(keyLocationToDelete);

        indexPageManager.deleteKey(keyLocationToDelete);
        LeafDataLocation borrowedLeafKeyLocation = leftLeafNode.leafKeys.removeLast();
        leftLeafNode.keysCache.remove(borrowedLeafKeyLocation);

        String borrowedKey = indexPageManager.getKeyToDataLocation(borrowedLeafKeyLocation).key;
        KeyLocation indexedKeyLocation = indexPageManager.addIndexedKey(new KeyItem(borrowedKey));
        parentNode.keysAndChildren.replaceWithNewKey(keyNumOfLeftChild, indexedKeyLocation);
        deficientLeafNode.leafKeys.addHead(borrowedLeafKeyLocation);
    }

    private static void borrowKeyFromRightLeafAndReplaceParentKey(TreeKeyNode parentNode,
                                                                  TreeLeafNode deficientLeafNode,
                                                                  TreeLeafNode rightLeafNode,
                                                                  IndexPageManager indexPageManager) throws IOException {
        int keyNumOfLeftChild = parentNode.keysAndChildren.getKeyNumOfLeftChild(deficientLeafNode.pageId);
        KeyLocation keyLocationToDelete = parentNode.keysAndChildren.getKeyLocation(keyNumOfLeftChild);
        parentNode.keysCache.remove(keyLocationToDelete);
// L:321 P:518 Pr:454 333[key0329]405[key0331]580[n]0[n]0[n]0[n]0 R:192
        indexPageManager.deleteKey(keyLocationToDelete);
        LeafDataLocation borrowedLeafKeyLocation = rightLeafNode.leafKeys.removeFirst();
        rightLeafNode.keysCache.remove(borrowedLeafKeyLocation);
        LeafDataLocation newRightLeafHeadKeyLocation = rightLeafNode.leafKeys.getHead();

        String newRightLeafHeadKey = indexPageManager.getKeyToDataLocation(newRightLeafHeadKeyLocation).key;
        KeyLocation indexedKeyLocation = indexPageManager.addIndexedKey(new KeyItem(newRightLeafHeadKey));

        parentNode.keysAndChildren.replaceWithNewKey(keyNumOfLeftChild, indexedKeyLocation);
        deficientLeafNode.leafKeys.addTail(borrowedLeafKeyLocation);
    }

    private static void moveAllLeafKeysToLeftLeafNode(TreeKeyNode parentNode,
                                                      TreeLeafNode deficientLeafNode,
                                                      TreeLeafNode leftLeafNode,
                                                      TreeLeafNode rightLeafNode,
                                                      IndexPageManager indexPageManager) throws IOException {
        int keyNumOfLeftChild = parentNode.keysAndChildren.getKeyNumOfLeftChild(leftLeafNode.pageId);
        KeyLocationAndRightChild keyLocationAndRightChild = parentNode.keysAndChildren.removeKeyAndRightChild(keyNumOfLeftChild);
        indexPageManager.deleteKey(keyLocationAndRightChild.keyLocation);
        parentNode.keysCache.remove(keyLocationAndRightChild.keyLocation);
        leftLeafNode.setRightSibling(deficientLeafNode.getRightSibling());
        if (rightLeafNode != null) {
            rightLeafNode.setLeftSibling(leftLeafNode.pageId);
        }
        leftLeafNode.leafKeys.addRightNode(deficientLeafNode.leafKeys);
        deficientLeafNode.setRightSibling(0);
        deficientLeafNode.setLeftSibling(0);
        deficientLeafNode.keysCache.clear();
        assert deficientLeafNode.leafKeys.isEmpty();
        assert !deficientLeafNode.leafKeys.isFull();
    }

    private static void moveAllLeafKeysToRightLeafNode(TreeKeyNode parentNode,
                                                       TreeLeafNode deficientLeafNode,
                                                       TreeLeafNode leftLeafNode,
                                                       TreeLeafNode rightLeafNode,
                                                       IndexPageManager indexPageManager) throws IOException {
        int keyNumOfLeftChild = parentNode.keysAndChildren.getKeyNumOfLeftChild(deficientLeafNode.pageId);
        KeyLocationAndLeftChild keyLocationAndRightChild = parentNode.keysAndChildren.removeKeyAndLeftChild(keyNumOfLeftChild);
        parentNode.keysCache.remove(keyLocationAndRightChild.keyLocation);
        indexPageManager.deleteKey(keyLocationAndRightChild.keyLocation);
        rightLeafNode.setLeftSibling(deficientLeafNode.getLeftSibling());
        if (leftLeafNode != null) {
            leftLeafNode.setRightSibling(rightLeafNode.pageId);
        }
        rightLeafNode.leafKeys.addLeftNode(deficientLeafNode.leafKeys);
        deficientLeafNode.setRightSibling(0);
        deficientLeafNode.setLeftSibling(0);
        deficientLeafNode.keysCache.clear();
        assert deficientLeafNode.leafKeys.isEmpty();
        assert !deficientLeafNode.leafKeys.isFull();
    }

    private static void updateChildWithNewParent(long parentPageId,
                                                 long childPageId,
                                                 boolean childIsLeaf,
                                                 TreeKeyNodePageManager treeKeyNodePageManager,
                                                 TreeLeafNodePageManager treeLeafNodePageManager) throws IOException {
        if (childIsLeaf) {
            CachedPageNew<TreeLeafNode> childPage = treeLeafNodePageManager.getAndLockWriteLeafNodePage(childPageId);
            TreeNode.setParent(childPage.getPage(), parentPageId);
            treeLeafNodePageManager.unlockPage(childPage);
        } else {
            CachedPageNew<TreeKeyNode> childPage = treeKeyNodePageManager.getAndLockWriteKeyNodePage(childPageId);
            TreeNode.setParent(childPage.getPage(), parentPageId);
            treeKeyNodePageManager.unlockPage(childPage);
        }


    }


    private static void updateChildrenWithNewParent(TreeKeyNode keyNode,
                                                    long newParentPageId,
                                                    boolean childIsLeaf,
                                                    TreeKeyNodePageManager treeKeyNodePageManager,
                                                    TreeLeafNodePageManager treeLeafNodePageManager) throws IOException {
        if (childIsLeaf) {
            { // updating most left child of first key
                long mostLeftChild = keyNode.keysAndChildren.getLeftChild(0);
                CachedPageNew<TreeLeafNode> newRightChildPage = treeLeafNodePageManager.getAndLockWriteLeafNodePage(mostLeftChild);
                TreeNode.setParent(newRightChildPage.getPage(), newParentPageId);
                treeLeafNodePageManager.unlockPage(newRightChildPage);
            }

            for (int i = 0; i < keyNode.keysAndChildren.getNextKeyNum(); i++) {
                CachedPageNew<TreeLeafNode> newRightChildPage = treeLeafNodePageManager.getAndLockWriteLeafNodePage(keyNode.keysAndChildren.getRightChild(i));
                TreeNode.setParent(newRightChildPage.getPage(), newParentPageId);
                treeLeafNodePageManager.unlockPage(newRightChildPage);
            }
        } else {
            { // updating most left child of first key
                long mostLeftChild = keyNode.keysAndChildren.getLeftChild(0);
                CachedPageNew<TreeKeyNode> newRightChildPage = treeKeyNodePageManager.getAndLockWriteKeyNodePage(mostLeftChild);
                TreeNode.setParent(newRightChildPage.getPage(), newParentPageId);
                treeKeyNodePageManager.unlockPage(newRightChildPage);
            }

            for (int i = 0; i < keyNode.keysAndChildren.getNextKeyNum(); i++) {
                CachedPageNew<TreeKeyNode> newRightChildPage = treeKeyNodePageManager.getAndLockWriteKeyNodePage(keyNode.keysAndChildren.getRightChild(i));
                TreeNode.setParent(newRightChildPage.getPage(), newParentPageId);
                treeKeyNodePageManager.unlockPage(newRightChildPage);
            }
        }

    }

}


























