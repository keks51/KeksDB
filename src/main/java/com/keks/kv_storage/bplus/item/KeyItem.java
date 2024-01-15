package com.keks.kv_storage.bplus.item;


import com.keks.kv_storage.Item;
import com.keks.kv_storage.TypeSize;

import java.nio.ByteBuffer;
import java.util.Objects;


public class KeyItem implements Item {

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

    public final String key;

    public KeyItem(String key) {
        this.key = key;
    }

    public KeyItem(ByteBuffer bb) {
        short keyLen = bb.getShort();
        byte[] keyBytes = new byte[keyLen];
        bb.get(keyBytes);
        this.key = new String(keyBytes);
    }

    @Override
    public int getLen() {
        return TypeSize.INT + key.length();
    }

    @Override
    public void copyToBB(ByteBuffer bb) {
        bb.putShort((short) key.length());
        bb.put(key.getBytes());
    }

    @Override
    public byte[] getBytes() {
        ByteBuffer bb = ByteBuffer.allocate(getLen());
        bb.putShort((short) key.length());
        bb.put(key.getBytes());
        return bb.array();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        KeyItem that = (KeyItem) o;
        return Objects.equals(key, that.key);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key);
    }

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer("KeyItem(");
        sb.append("key='").append(key).append('\'');
        sb.append(')');
        return sb.toString();
    }
}
