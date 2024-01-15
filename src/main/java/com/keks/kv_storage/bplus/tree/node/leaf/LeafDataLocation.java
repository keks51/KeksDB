package com.keks.kv_storage.bplus.tree.node.leaf;

import com.keks.kv_storage.bplus.page_manager.page_disk.sp.SlotLocation;

import java.nio.ByteBuffer;


public class LeafDataLocation extends SlotLocation {

    public LeafDataLocation(long pageId, short slotId) {
        super(pageId, slotId);
    }

    public LeafDataLocation(ByteBuffer bb) {
        super(bb);
    }

}