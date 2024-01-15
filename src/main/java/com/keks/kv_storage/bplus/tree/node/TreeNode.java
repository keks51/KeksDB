package com.keks.kv_storage.bplus.tree.node;


import com.keks.kv_storage.bplus.conf.BtreeConf;
import com.keks.kv_storage.bplus.page_manager.managers.IndexPageManager;
import com.keks.kv_storage.bplus.page_manager.page_disk.fixed.TreeNodePage;
import com.keks.kv_storage.bplus.page_manager.page_disk.sp.SlotLocation;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;


public abstract class TreeNode<T extends SlotLocation> extends TreeNodePage {

    public final long pageId;
    public final BtreeConf btreeConf;
    private final NodePointer parent;
    private final NodePointer leftSibling;
    private final NodePointer rightSibling;
    protected final KeysBinarySearch<T> keys;

    public final ConcurrentHashMap<T, String> keysCache;

    public TreeNode(long pageId,
                    BtreeConf btreeConf,
                    ConcurrentHashMap<T, String> keysCache) {
        super(pageId);
        this.pageId = pageId;
        this.btreeConf = btreeConf;
        this.parent = new NodePointer(super.getParentPageBuf());
        this.leftSibling = new NodePointer(super.getLeftSiblingPageBuf());
        this.rightSibling = new NodePointer(super.getRightSiblingPageBuf());
        this.keysCache = keysCache;
        this.keys = buildKeys(keysCache);
    }

    public TreeNode(ByteBuffer bb,
                    BtreeConf btreeConf,
                    ConcurrentHashMap<T, String> keysCache) {
        super(bb);
        this.pageId = super.pageId;
//        System.out.println("here123: " + pageId);
        this.btreeConf = btreeConf;
        this.parent = new NodePointer(super.getParentPageBuf());
        this.leftSibling = new NodePointer(super.getLeftSiblingPageBuf());
        this.rightSibling = new NodePointer(super.getRightSiblingPageBuf());
        this.keysCache = keysCache;
        this.keys = buildKeys(keysCache);
    }


    protected abstract KeysBinarySearch<T> buildKeys(ConcurrentHashMap<T, String> keysCache);

    public long getParent() {
        return parent.get();
    }

    public void setParent(long pageId) {
        parent.set(pageId);
    }

    public long getLeftSibling() {
        return leftSibling.get();
    }

    public void setLeftSibling(long pageId) {
        leftSibling.set(pageId);
    }

    public long getRightSibling() {
        return rightSibling.get();
    }

    public void setRightSibling(long pageId) {
        rightSibling.set(pageId);
    }

    public abstract String getKey(T slotLocation, IndexPageManager indexPageManager) throws IOException;

    public int getInsertPos(String key, IndexPageManager indexPageManager) throws IOException {
        try {
            int low = 0;
            int high = keys.getNextKeyNum() - 1;
//            printThread(high + "");
            while (low <= high) {
                int mid = (low + high) >>> 1;
                T keyLocation = keys.getKeyLocation(mid);
//                printThread(keyLocation.toString());
                String midKey = getKey(keyLocation, indexPageManager);
                int comp = midKey.compareTo(key);
                if (comp < 0)
                    low = mid + 1;
                else if (comp > 0)
                    high = mid - 1;
                else {
                    return mid;
                }
            }
            return -(low + 1);
        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(1);
            return -1;
        }

    }

    public T getKeyLocation(String key, IndexPageManager indexPageManager) throws IOException {
        int keyNum = getInsertPos(key, indexPageManager);
        if (keyNum < 0) {
            return null;
        } else {
            return keys.getKeyLocation(keyNum);
        }
    }

    public static void setParent(TreeNodePage page, long newParentPageId) {
        new NodePointer(page.getParentPageBuf()).set(newParentPageId);
    }

    public static void printThread(String msg) {
        System.out.println("Thread[" + Thread.currentThread().getId() + "]  BPlusKVTableUtils: " + msg);
    }
}
