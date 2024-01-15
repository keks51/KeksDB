package com.keks.kv_storage.bplus.item;

import com.keks.kv_storage.record.KVRecord;
import com.keks.kv_storage.record.KvRow;

import java.nio.ByteBuffer;


public class KVRecordSplitsBuilder {

    private final ByteBuffer bb;
    private ByteBuffer dataBuffer;
    private final int tupleLen;
    private String key;
    private int keyLeftToRead;
    private ByteBuffer keyBuffer;

    public KVRecordSplitsBuilder(int len) {
        this.tupleLen = len;
        this.bb = ByteBuffer.allocate(tupleLen);
    }

    public void add(ByteBuffer bb) {
        this.bb.put(bb);
    }

    public KVRecord buildKVRecord() {
        bb.position(0);
        KvRow kvRow = KvRow.fromByteBuffer(bb);
        return new KVRecord(kvRow.keyBytes[0], kvRow.valueBytes[0]);
//        return KVRecord.fromByteBuffer(byteBuffer);
    }

}
