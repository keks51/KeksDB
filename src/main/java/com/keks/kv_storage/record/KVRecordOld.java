package com.keks.kv_storage.record;


import com.keks.kv_storage.Item;
import com.keks.kv_storage.TypeSize;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;


public class KVRecordOld implements Item {

    @Override
    public int getMinSize() {
        return TypeSize.INT;
    }

    @Override
    public int getTotalLen(ByteBuffer bb) {
        try {
            bb.mark();
            return bb.getInt();
        } finally {
            bb.reset();
        }
    }

    private final int len;

    public final String key;

    public final byte[] valueBytes;

    public KVRecordOld(String key) {
       this(key, "".getBytes());
    }

    public KVRecordOld(String key, String value) {
        this(key, value.getBytes());
    }
    public KVRecordOld(String key, byte[] valueBytes) {
        this.key = key;
        this.valueBytes = valueBytes;
        this.len = TypeSize.INT + TypeSize.SHORT + key.length() + TypeSize.INT + valueBytes.length;
    }

    public static KVRecordOld fromByteBuffer(ByteBuffer bb) {
        int len = bb.getInt();
        if (bb.remaining() + TypeSize.INT < len) throw new RuntimeException();

        short keyLen = bb.getShort();
        byte[] keyBytes = new byte[keyLen];
        bb.get(keyBytes);
        String key = new String(keyBytes);
        int valueLen = bb.getInt();
        byte[] valueBytes = new byte[valueLen];
        bb.get(valueBytes);

        return new KVRecordOld(key, valueBytes);
    }

    public boolean isDeleted() {
        return valueBytes.length == 0;
    }

    // TODO rename to itemLen
    @Override
    public int getLen() {
        return len;
    }

    @Override
    public void copyToBB(ByteBuffer bb) {
        bb.putInt(len);
        bb.putShort((short) key.length());
        bb.put(key.getBytes());
        bb.putInt(valueBytes.length);
        bb.put(valueBytes);
    }

    @Override
    public byte[] getBytes() {
        ByteBuffer bb = ByteBuffer.allocate(len);
        copyToBB(bb);
        return bb.array();
    }

    @Override
    public String toString() {
        return "KVRecord(len=" + len + ",key=" + key + ",value=" + new String(valueBytes) + ")";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        KVRecordOld kvRecord = (KVRecordOld) o;
        return len == kvRecord.len && key.equals(kvRecord.key) && Arrays.equals(valueBytes, kvRecord.valueBytes);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(len, key);
        result = 31 * result + Arrays.hashCode(valueBytes);
        return result;
    }

}
