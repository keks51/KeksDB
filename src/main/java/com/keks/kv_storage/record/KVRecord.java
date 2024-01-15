package com.keks.kv_storage.record;


import com.keks.kv_storage.TypeSize;


public class KVRecord extends KvRow {

    public final String key;

    public final byte[] valueBytes;


    public KVRecord(String key) {
        this(key, "".getBytes());
    }

    public KVRecord(String key, String value) {
        this(key, value.getBytes());
    }

    public KVRecord(String key, byte[] valueBytes) {
        super(
                TypeSize.SHORT + key.length(),
                TypeSize.INT + valueBytes.length,
                new byte[][]{key.getBytes()},
                new byte[][]{valueBytes}
        );
        this.valueBytes = valueBytes;
        this.key = key;
    }

    public KVRecord(byte[] key, byte[] valueBytes) {
        super(
                TypeSize.SHORT + key.length,
                TypeSize.INT + valueBytes.length,
                new byte[][]{key},
                new byte[][]{valueBytes}
        );
        this.valueBytes = valueBytes;
        this.key = new String(key);
    }

    public boolean isDeleted() {
        return valueBytes.length == 0;
    }

    @Override
    public String toString() {
        return "KVRecord(len=" + getLen() + ",key=" + key + ",value=" + new String(valueBytes) + ")";
    }

}
