package com.keks.kv_storage.lsm.ss_table;

import com.keks.kv_storage.Item;
import com.keks.kv_storage.TypeSize;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Objects;


public class DenseIndexBlock implements Item {

    private final byte[] offsets;

    private final byte[] keys;

    private final int totalLen;


    public DenseIndexBlock(byte[] offsets, byte[] keys) {
        this.offsets = offsets;
        this.keys = keys;
        //                 total_len, offsets_len, offset_data_len, keys_len, keys_data_len
        this.totalLen = TypeSize.INT + TypeSize.INT + offsets.length + TypeSize.INT + keys.length;
    }

    public static DenseIndexBlock fromByteBuffer(ByteBuffer bb) {
        int totalLen = bb.getInt();
        if (bb.remaining() + TypeSize.INT < totalLen) throw new RuntimeException();
        int offsetsLen = bb.getInt();
        byte[] offsets = new byte[offsetsLen];
        bb.get(offsets);
        int keysLen = bb.getInt();
        byte[] keys = new byte[keysLen];
        bb.get(keys);
        return new DenseIndexBlock(offsets, keys);
    }


    public int getKeysNum() {
        return offsets.length / TypeSize.INT;
    }

    public IndexedKey getKey(int pos) {
        int posInKeys = ByteBuffer.wrap(offsets).position(pos * TypeSize.INT).getInt();
        return IndexedKey.fromByteBuffer(ByteBuffer.wrap(keys).position(posInKeys));
    }

    public static class DenseIndexBlockBuilder {

        private final ByteBuffer offsets;
        private final ArrayList<IndexedKey> keys;

        private int sumSize = 0;

        public DenseIndexBlockBuilder(int indexBlockSize) {
            offsets = ByteBuffer.allocate(indexBlockSize * (TypeSize.INT));
            keys = new ArrayList<>(indexBlockSize);
        }

        public DenseIndexBlockBuilder add(IndexedKey key) {
            offsets.putInt(sumSize);
            sumSize += key.getLen();
            keys.add(key);
            return this;
        }

        public DenseIndexBlock build() {
            ByteBuffer keysBB = ByteBuffer.allocate(sumSize);
            for (IndexedKey key : keys) {
                keysBB.put(key.getBytes());
            }

            // if block was not fully loaded. For example the last block doesn't contain all block idx like 8192 bun only 143
            if (offsets.remaining() > 0) {
                byte[] bytes = new byte[offsets.limit() - offsets.remaining()];
                offsets.position(0);
                offsets.get(bytes);
                return new DenseIndexBlock(bytes, keysBB.array());
            } else {
                return new DenseIndexBlock(offsets.array(), keysBB.array());
            }
        }

    }

    public IndexedKey findKey(String key) {
        int low = 0;
        int high = getKeysNum() - 1;
        while (low <= high) {
            int mid = (low + high) >>> 1;
            IndexedKey midIndexKey = getKey(mid);
            String midKey = midIndexKey.key;
            int comp = midKey.compareTo(key);
            if (comp < 0)
                low = mid + 1;
            else if (comp > 0)
                high = mid - 1;
            else {
                return midIndexKey;
            }
        }
        return null;
    }


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

    @Override
    public int getLen() {
        return totalLen;
    }

    @Override
    public void copyToBB(ByteBuffer bb) {
        bb.putInt(totalLen);
        bb.putInt(offsets.length);
        bb.put(offsets);
        bb.putInt(keys.length);
        bb.put(keys);
    }

    @Override
    public byte[] getBytes() {
        ByteBuffer bb = ByteBuffer.allocate(totalLen);
        copyToBB(bb);
        return bb.array();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DenseIndexBlock that = (DenseIndexBlock) o;
        return totalLen == that.totalLen && Arrays.equals(offsets, that.offsets) && Arrays.equals(keys, that.keys);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(totalLen);
        result = 31 * result + Arrays.hashCode(offsets);
        result = 31 * result + Arrays.hashCode(keys);
        return result;
    }

}
