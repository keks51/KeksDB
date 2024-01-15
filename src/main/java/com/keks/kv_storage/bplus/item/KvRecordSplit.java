package com.keks.kv_storage.bplus.item;

import com.keks.kv_storage.Item;
import com.keks.kv_storage.ex.NotImplementedException;

import java.nio.ByteBuffer;


public class KvRecordSplit implements Item {

    private final byte[] data;

    public KvRecordSplit(byte[] data) {
        this.data = data;
    }

    public KvRecordSplit(ByteBuffer bb) {
        bb.reset();
        this.data = new byte[bb.remaining()];
        bb.get(data);
    }

    @Override
    public int getLen() {
        return data.length;
    }

    @Override
    public void copyToBB(ByteBuffer bb) {
        bb.put(data);
    }

    @Override
    public byte[] getBytes() {
        return data;
    }

    @Override
    public String toString() {
        return "DataBlock(" + "dataLen=" + getLen() + ')';
    }

    @Override
    public int getMinSize() {
        throw new NotImplementedException();
    }

    @Override
    public int getTotalLen(ByteBuffer bb) {
        throw new NotImplementedException();
    }

}
