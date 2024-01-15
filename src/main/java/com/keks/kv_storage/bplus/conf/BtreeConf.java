package com.keks.kv_storage.bplus.conf;

import com.keks.kv_storage.TypeSize;
import com.keks.kv_storage.bplus.page_manager.page_disk.fixed.TreeNodePage;
import com.keks.kv_storage.bplus.page_manager.page_disk.sp.SlotLocation;
import com.keks.kv_storage.bplus.tree.utils.BtreeUtils;


public class BtreeConf {

    public final int treeOrder;
    public final int maxKeys;

    public final int minChildren;
    public final int minKeys;
    public final int leafNodeKVArrSize;
    public final int maxChildren;

    public final int keysArrMidPointPos;
    public static final int MAX_ORDER = (TreeNodePage.DEFAULT_PAGE_SIZE - TreeNodePage.KEYS_ARR_POS) / (SlotLocation.SIZE + TypeSize.LONG) - 1;

    public static final long DEFAULT_PAGE_BUFFER_SIZE_BYTES = 40_000_000L;

    public BtreeConf(int treeOrder) { // max is 451
        if (treeOrder > MAX_ORDER) throw new IllegalArgumentException("TreeOrder: " + treeOrder + " greater then max: " + MAX_ORDER);
        this.treeOrder = treeOrder;
        this.maxKeys = treeOrder - 1;
        this.leafNodeKVArrSize = treeOrder;
        this.keysArrMidPointPos = BtreeUtils.getArrMidpoint(leafNodeKVArrSize);
        this.maxChildren = treeOrder;
        this.minChildren = (int) Math.ceil(treeOrder / 2.0);
        this.minKeys = minChildren - 1;

    }

}
