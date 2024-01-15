package com.keks.kv_storage.bplus.page_manager;


import java.nio.ByteBuffer;


public abstract class Page {

    public static final int SLOTTED_PAGE_TYPE = 1;
    public static final int FIXED_SIZE_PAGE_TYPE = 2;
    public static final int BIT_MASK_PAGE_TYPE = 3;
    public static final int BLOCK_PAGE_TYPE = 4;
    public static final int INDEXES_PAGE_TYPE = 5;


    public final Long pageId;


    public Page(Long pageId) {
        this.pageId = pageId;
    }

    public Page(ByteBuffer bb) {
        bb.getShort(); // skipping page type since we don't need this information here
        this.pageId = bb.getLong();
    }

    public abstract byte[] toBytes();

    public abstract boolean isFull();

}
