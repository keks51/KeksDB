package com.keks.kv_storage.bplus.tree;


import com.keks.kv_storage.TypeSize;
import com.keks.kv_storage.Item;
import com.keks.kv_storage.bplus.tree.node.leaf.LeafDataLocation;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;


public class KeyToDataLocationsItem implements Item {

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
    public final int dataLen;
    public final LeafDataLocation[] dataLocations;

    public KeyToDataLocationsItem(String key, int dataLen, LeafDataLocation[] dataLocations) {
        this.key = key;
        this.dataLocations = dataLocations;
        this.dataLen = dataLen;
    }

    public KeyToDataLocationsItem(ByteBuffer bb) {
        short keyLen = bb.getShort();
        byte[] keyBytes = new byte[keyLen];
        bb.get(keyBytes);
        this.key = new String(keyBytes);
        this.dataLen = bb.getInt();
        this.dataLocations = new LeafDataLocation[bb.remaining() / LeafDataLocation.SIZE];
        for (int i = 0; i < dataLocations.length; i++) {
            dataLocations[i] = new LeafDataLocation(bb.getLong(), bb.getShort());
        }
    }

    @Override
    public int getLen() {
        return TypeSize.INT + key.length() + TypeSize.INT + dataLocations.length * LeafDataLocation.SIZE;
    }

    @Override
    public void copyToBB(ByteBuffer bb) {
        bb.putShort((short) key.length());
        bb.put(key.getBytes());
        bb.putInt(dataLen);
        for (LeafDataLocation location : dataLocations) {
            bb.put(location.getBytes());
        }
    }

    @Override
    public byte[] getBytes() {
        ByteBuffer bb = ByteBuffer.allocate(getLen());
        bb.putShort((short) key.length());
        bb.put(key.getBytes());
        bb.putInt(dataLen);
        for (LeafDataLocation location : dataLocations) {
            bb.put(location.getBytes());
        }
        return bb.array();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        KeyToDataLocationsItem that = (KeyToDataLocationsItem) o;
        return Objects.equals(key, that.key) && Objects.equals(dataLen, that.dataLen) && Arrays.equals(dataLocations, that.dataLocations);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(key);
        result = 31 * result + Arrays.hashCode(dataLocations);
        return result;
    }

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer("KeyDataTupleLocations{");
        sb.append("key='").append(key).append('\'');
        sb.append(", dataLen=").append(dataLen);
        sb.append(", dataLocations=").append(dataLocations == null ? "null" : Arrays.asList(dataLocations).toString());
        sb.append('}');
        return sb.toString();
    }
}
