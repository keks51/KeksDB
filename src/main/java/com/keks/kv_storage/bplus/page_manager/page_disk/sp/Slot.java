package com.keks.kv_storage.bplus.page_manager.page_disk.sp;

import com.keks.kv_storage.TypeSize;

import java.nio.ByteBuffer;


public class Slot implements Storable {

    static public final int SIZE = TypeSize.SHORT * 2;

    private short startPos;

    private final short length;

//    private short isDeleted;

    public Slot() {
        startPos = 0;
        length = 0;
    }

    public Slot(int startPos, int length) {
        this.startPos = (short) startPos;
        this.length = (short) length;
    }

    public Slot(Slot slot) {
        startPos = slot.startPos;
        length = slot.length;
    }

    // TODO check is deleted
    public Slot(ByteBuffer bb) {
        startPos = bb.getShort();
        length = bb.getShort();
    }

    public final void store(ByteBuffer bb) {
        bb.putShort(startPos);
        bb.putShort(length);
    }

    public void setIsDeleted() {
        this.startPos = -1;
    }

    public boolean isDeleted() {
        return startPos == -1;
    }

    public final int getStoredLength() {
        return SIZE;
    }

    final int getItemLength() {
        return length;
    }

    final int getItemStartPos() {
        return startPos;
    }

    public final StringBuilder appendTo(StringBuilder sb) {
        sb.append("slot(startPos=").append(startPos)
                .append(", length=").append(length)
                .append(", isDeleted=").append(isDeleted())
                .append(")");
        return sb;
    }

    @Override
    public final String toString() {
        return appendTo(new StringBuilder()).toString();
    }
}
