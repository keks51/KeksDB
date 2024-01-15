package com.keks.kv_storage.bplus.tree.node.key;

import com.keks.kv_storage.bplus.page_manager.page_disk.sp.SlotLocation;

import java.nio.ByteBuffer;


public class KeyLocation extends SlotLocation  {

    public KeyLocation(long pageId, short slotId) {
        super(pageId, slotId);
    }

    public KeyLocation(ByteBuffer bb) {
        super(bb);
    }

}
