package com.keks.kv_storage.lsm.ss_table;

import com.keks.kv_storage.TypeSize;
import com.keks.kv_storage.Item;

import java.nio.ByteBuffer;
import java.util.Objects;


public class IndexedKey implements Item {

    // TODO indexed key should be composed of several keys (several columns)
    private final short len;
    public final int refRecordLen;

    public final String key;
    public final long posInReferenceFile;

    public IndexedKey(String key, long posInReferenceFile, int refRecordLen) {
        this.key = key;
        this.posInReferenceFile = posInReferenceFile;
        this.len = (short) (TypeSize.SHORT + TypeSize.SHORT + key.length() + TypeSize.LONG + TypeSize.INT); // indexRecLen + KeyLen + key + PosInRefFile + DataRefLen
        this.refRecordLen = refRecordLen;
    }

    public static IndexedKey fromByteBuffer(ByteBuffer bb) {
        short len = bb.getShort();
        if (bb.remaining() + TypeSize.SHORT < len) throw new RuntimeException();
        short keyLen = bb.getShort();
        byte[] keyBytes = new byte[keyLen];
        bb.get(keyBytes);

        long posInReferenceFile = bb.getLong();
        int refRecordLen = bb.getInt();
        return new IndexedKey(new String(keyBytes), posInReferenceFile, refRecordLen);
    }

    @Override
    public int getMinSize() {
        return TypeSize.SHORT;
    }

    @Override
    public int getTotalLen(ByteBuffer bb) {
        try {
            bb.mark();
            return bb.getShort();
        } finally {
            bb.reset();
        }
    }

    @Override
    public int getLen() {
        return len;
    }

    @Override
    public void copyToBB(ByteBuffer bb) {
        bb.putShort(len); // indexRecLen
        bb.putShort((short) (key.length())); // keyLen
        bb.put(key.getBytes()); // key
        bb.putLong(posInReferenceFile); // ref pos
        bb.putInt(refRecordLen); // ref rec len
    }

    @Override
    public byte[] getBytes() {
        ByteBuffer allocate = ByteBuffer.allocate(len);
        copyToBB(allocate);
        return allocate.array();
    }

    @Override
    public String toString() {
        return "IndexedKey(len=" + len + ",key=" + key + ",posInRefFile=" + posInReferenceFile + ",refRecordLen=" + refRecordLen + ")";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        IndexedKey that = (IndexedKey) o;
        return len == that.len && refRecordLen == that.refRecordLen && posInReferenceFile == that.posInReferenceFile && key.equals(that.key);
    }

    @Override
    public int hashCode() {
        return Objects.hash(len, refRecordLen, key, posInReferenceFile);
    }

}
