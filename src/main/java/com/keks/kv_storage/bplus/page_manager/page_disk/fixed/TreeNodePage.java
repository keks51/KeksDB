package com.keks.kv_storage.bplus.page_manager.page_disk.fixed;

import com.keks.kv_storage.TypeSize;
import com.keks.kv_storage.bplus.page_manager.Page;

import java.nio.ByteBuffer;


public class TreeNodePage extends Page {

    public static final short pageType = BLOCK_PAGE_TYPE;

    private static final int PAGE_TYPE_OVERHEAD = TypeSize.SHORT;
    private static final int ID_OVERHEAD = TypeSize.LONG;
    private static final int IS_LEAF_OVERHEAD = TypeSize.SHORT;
    private static final int TREE_ORDER_OVERHEAD = TypeSize.SHORT;

    private byte[] data;
    public static final int BLOCK_START_POS = PAGE_TYPE_OVERHEAD + ID_OVERHEAD + IS_LEAF_OVERHEAD + TREE_ORDER_OVERHEAD;

    public static final int NEXT_KEY_POS = BLOCK_START_POS + TypeSize.SHORT;
    public static final int PARENT_PAGE = NEXT_KEY_POS + TypeSize.INT;
    public static final int LEFT_SIBLING_PAGE = PARENT_PAGE + TypeSize.LONG ;
    public static final int RIGHT_SIBLING_PAGE = LEFT_SIBLING_PAGE + TypeSize.LONG;
    public static final int KEYS_ARR_POS = RIGHT_SIBLING_PAGE + TypeSize.LONG;

    public static final int DEFAULT_PAGE_SIZE = 8 * 1024; // 8192

    private short isLeaf = 1;
    private short treeOrder = -1;

    public TreeNodePage(long pageId) {
        super(pageId);
        this.data = new byte[DEFAULT_PAGE_SIZE];
    }

    public TreeNodePage(ByteBuffer bb) {
        super(bb);
        this.isLeaf = bb.getShort();
        this.treeOrder = bb.getShort();
        this.data = bb.array();
    }

    @Override
    public byte[] toBytes() {
        ByteBuffer bb = ByteBuffer.wrap(data);
        bb.putShort(pageType);
        bb.putLong(pageId);
        bb.putShort(isLeaf);
        bb.putShort(treeOrder);
        return data;
    }

    public ByteBuffer getBlock() {
        ByteBuffer bb = ByteBuffer.wrap(data, BLOCK_START_POS, DEFAULT_PAGE_SIZE - BLOCK_START_POS);
        bb.mark();
        return bb;
    }

    public void emptyPage() {
        data = new byte[DEFAULT_PAGE_SIZE];
    }

    public int getTreeOrder() {
        return treeOrder;
    }

    public void setTreeOrder(int order) {
        treeOrder = (short) order;
    }

    public ByteBuffer getNextKeyBuf() {
        return getBB(NEXT_KEY_POS, TypeSize.INT);
    }

    public ByteBuffer getParentPageBuf() {
        return getBB(PARENT_PAGE, TypeSize.LONG);
    }

    public ByteBuffer getLeftSiblingPageBuf() {
        return getBB(LEFT_SIBLING_PAGE, TypeSize.LONG);
    }

    public ByteBuffer getRightSiblingPageBuf() {
        return getBB(RIGHT_SIBLING_PAGE, TypeSize.LONG);
    }

    public ByteBuffer getKeysBuf() {
        return getBB(KEYS_ARR_POS, DEFAULT_PAGE_SIZE - KEYS_ARR_POS);
    }

    private ByteBuffer getBB(int startPos, int len) {
        ByteBuffer bb = ByteBuffer.wrap(data, startPos, len);
        bb.mark();
        return bb;
    }

    public boolean isLeafNode() {
        return isLeaf == 1;
    }

    public void setAsLeafNode() {
        isLeaf = 1;
    }

    public void setAsKeyNode() {
        isLeaf = 0;
    }

    // do not change it. this is used to mark as free when writing or reading
    @Override
    public boolean isFull() {
        return getNextKeyBuf().getInt() != 0;
    }

    public boolean isEmpty() {
        return getNextKeyBuf().getInt() == 0;
    }
}
