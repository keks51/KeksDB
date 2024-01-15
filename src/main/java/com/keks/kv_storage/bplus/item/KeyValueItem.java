package com.keks.kv_storage.bplus.item;

import com.keks.kv_storage.Item;
import com.keks.kv_storage.TypeSize;

import java.nio.ByteBuffer;


public class KeyValueItem implements Item {

    public static final int KEY_LEN_OVERHEAD = TypeSize.SHORT;

    public final String key;
    public final String value;

    public KeyValueItem(String key, String value) {
        this.key = key;
        this.value = value;
    }

    public KeyValueItem(ByteBuffer bb) {
        short keyLen = bb.getShort();
        byte[] keyBytes = new byte[keyLen];
        bb.get(keyBytes);
        this.key = new String(keyBytes);
        byte[] valueBytes = new byte[bb.remaining()];
        bb.get(valueBytes);
        this.value = new String(valueBytes);
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
        return KEY_LEN_OVERHEAD + key.length() + value.length();
    }

    @Override
    public void copyToBB(ByteBuffer bb) {
        bb.putShort((short) key.length());
        bb.put(key.getBytes());
        bb.put(value.getBytes());
    }

    @Override
    public byte[] getBytes() {
        ByteBuffer bb = ByteBuffer.allocate(getLen());
        copyToBB(bb);
        return bb.array();
    }
}
