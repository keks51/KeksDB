package com.keks.kv_storage.bplus.page_manager.page_disk.sp;


import com.keks.kv_storage.TypeSize;

import java.nio.ByteBuffer;
import java.util.Objects;


public class SlotLocation {

    public static final int SIZE = TypeSize.LONG + TypeSize.SHORT;

    public final long pageId;
    public final short slotId;

    public SlotLocation(long pageId, short slotId) {
        this.pageId = pageId;
        this.slotId = slotId;
    }

    public SlotLocation(ByteBuffer bb) {
        this.pageId = bb.getLong();
        this.slotId = bb.getShort();
    }

    public byte[] getBytes() {
        ByteBuffer bb = ByteBuffer.allocate(SIZE);
        bb.putLong(pageId);
        bb.putShort(slotId);
        return bb.array();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SlotLocation location = (SlotLocation) o;
        return pageId == location.pageId && slotId == location.slotId;
    }

    @Override
    public int hashCode() {
        return Objects.hash(pageId, slotId);
    }

    @Override
    public String toString() {
        return "Location(" + "pageId=" + pageId +
                ", slotId=" + slotId +
                ')';
    }

}
