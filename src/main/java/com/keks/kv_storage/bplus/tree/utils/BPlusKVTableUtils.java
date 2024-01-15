package com.keks.kv_storage.bplus.tree.utils;

import com.keks.kv_storage.bplus.buffer.CachedPageNew;
import com.keks.kv_storage.record.KVRecord;
import com.keks.kv_storage.bplus.page_manager.managers.DataPageManager;
import com.keks.kv_storage.bplus.page_manager.managers.IndexPageManager;
import com.keks.kv_storage.bplus.page_manager.managers.TreeKeyNodePageManager;
import com.keks.kv_storage.bplus.page_manager.managers.TreeLeafNodePageManager;
import com.keks.kv_storage.bplus.tree.node.key.TreeKeyNode;
import com.keks.kv_storage.bplus.tree.node.leaf.LeafDataLocation;
import com.keks.kv_storage.bplus.tree.node.leaf.TreeLeafNode;
import com.keks.kv_storage.bplus.tree.node.traverse.TraverseKeyNode;
import com.keks.kv_storage.bplus.tree.node.traverse.TraverseLeafNode;

import java.io.IOException;


public class BPlusKVTableUtils {

    public static TraverseKeyNode getKeyNodeTraverseItem(CachedPageNew<TreeKeyNode> keyNodePage,
                                                         TreeKeyNodePageManager treeKeyNodePageManager,
                                                         boolean childIsLeaf) throws IOException {
        long leftSiblingPageId = keyNodePage.page.getLeftSibling();
        long rightSiblingPageId = keyNodePage.page.getRightSibling();

        CachedPageNew<TreeKeyNode> leftLeafSibling = null;
        CachedPageNew<TreeKeyNode> rightLeafSibling = null;
        if (leftSiblingPageId != 0) {
//            printThread("LeftSib: " + leftSiblingPageId, true);
            leftLeafSibling = treeKeyNodePageManager.getAndLockWriteKeyNodePage(leftSiblingPageId);
        }
        if (rightSiblingPageId != 0) {
            rightLeafSibling = treeKeyNodePageManager.getAndLockWriteKeyNodePage(rightSiblingPageId);
        }
        return new TraverseKeyNode(keyNodePage, leftLeafSibling, rightLeafSibling, childIsLeaf);
    }

    public static TraverseLeafNode getLeafNodeTraverseItem(CachedPageNew<TreeLeafNode> nodePage,
                                                           TreeLeafNode treeNode,
                                                           TreeLeafNodePageManager treeLeafNodePageManager) throws IOException {
        long leftSiblingPageId = treeNode.getLeftSibling();
        long rightSiblingPageId = treeNode.getRightSibling();

        CachedPageNew<TreeLeafNode> leftLeafSibling = null;
        CachedPageNew<TreeLeafNode> rightLeafSibling = null;
        if (leftSiblingPageId != 0) {
//            printThread("LeftSib: " + leftSiblingPageId, true);
            leftLeafSibling = treeLeafNodePageManager.getAndLockWriteLeafNodePage(leftSiblingPageId);
        }
        if (rightSiblingPageId != 0) {
            rightLeafSibling = treeLeafNodePageManager.getAndLockWriteLeafNodePage(rightSiblingPageId);
        }
        return new TraverseLeafNode(nodePage, leftLeafSibling, rightLeafSibling);
    }

//    public static <T extends SlotLocation> TraverseItem getNodeTraverseItem(CachedPageNew<TreeNodePage> nodePage,
//                                                                            TreeNode<T> treeNode,
//                                                                            TreeNodePageManager treeNodePageManager) throws IOException {
//        long leftSiblingPageId = treeNode.getLeftSibling();
//        long rightSiblingPageId = treeNode.getRightSibling();
//
//        CachedPageNew<TreeNodePage> leftLeafSibling = null;
//        CachedPageNew<TreeNodePage> rightLeafSibling = null;
//        if (leftSiblingPageId != 0) {
////            printThread("LeftSib: " + leftSiblingPageId, true);
//            leftLeafSibling = treeNodePageManager.getAndLockWritePage(leftSiblingPageId);
//        }
//        if (rightSiblingPageId != 0) {
//            rightLeafSibling = treeNodePageManager.getAndLockWritePage(rightSiblingPageId);
//        }
//        return new TraverseItem(nodePage, leftLeafSibling, rightLeafSibling);
//    }

    public static KVRecord getRecordFromLeaf(long leafPageId,
                                                IndexPageManager indexPageManager,
                                                TreeLeafNodePageManager treeLeafNodePageManager,
                                                DataPageManager dataPageManager,
                                                String key) throws IOException {
        CachedPageNew<TreeLeafNode> leafPage = treeLeafNodePageManager.getAndLockReadLeafNodePage(leafPageId);
        TreeLeafNode treeLeafNode = leafPage.page;
        LeafDataLocation location = treeLeafNode.getKeyLocation(key, indexPageManager);
        if (location == null) {
            treeLeafNodePageManager.unlockPage(leafPage);
            return null;
        } else {
            KVRecord res = dataPageManager.getKeyDataTuple(indexPageManager.getKeyToDataLocation(location));
            treeLeafNodePageManager.unlockPage(leafPage);
            return res;
        }
    }

    public static long getLeafPageId(String key,
                                     long rootPageId,
                                     int cnt,
                                     TreeKeyNodePageManager treeKeyNodePageManager,
                                     IndexPageManager indexPageManager) throws IOException {

        try {
            long childPageId = rootPageId;
            int treeHeight = treeKeyNodePageManager.bplusTreeRuntimeParameters.getTreeHeight();
            int level = 1;

            while (level < treeHeight) {
                level++;
                CachedPageNew<TreeKeyNode> keyNodePage = treeKeyNodePageManager.getAndLockReadKeyNodePage(childPageId);
                TreeKeyNode treeKeyNode = keyNodePage.getPage();
                childPageId = treeKeyNode.getChildPageId(key, indexPageManager);
                cnt++;
                treeKeyNodePageManager.unlockPage(keyNodePage);
            }
            return childPageId;
        } catch (Throwable t) {

            printThread("Failed to find key4: " + key);
            System.out.println("end1");
            t.printStackTrace();
            throw t;
        }
    }

    public static void printThread(String msg) {
        System.out.println("Thread[" + Thread.currentThread().getId() + "]  BPlusKVTableUtils: " + msg);
    }
}


























