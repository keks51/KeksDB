package com.keks.kv_storage.bplus.tree.node;

import java.nio.ByteBuffer;


public class NodePointer {

    private final ByteBuffer bb;

    public NodePointer(ByteBuffer bb) {
        this.bb = bb;
    }

    public void set(long pageId) {
        bb.reset();
        bb.putLong(pageId);
        bb.reset();
    }

    public long get() {
        bb.reset();
        long pageId = bb.getLong();
        bb.reset();
        return pageId;
    }

}