package com.keks.kv_storage.bplus.item;

import com.keks.kv_storage.record.KVRecord;

import java.nio.ByteBuffer;

public class KVRecordSplitter {

//    private final int tupleLen;
//    private final String key;
//    private final KVRecord record;
    private final byte[] recordBytes;
    private final int recordLen;

    public KVRecordSplitter(KVRecord record) {
        this.recordLen = record.getLen();
        this.recordBytes = record.getBytes();
//        this.tupleLen = tupleLen;
//        this.key = key;
//        this.keyBuffer = ByteBuffer.wrap(key.getBytes());
//        this.dataBytesBuffer = ByteBuffer.wrap(valueBytes);
    }

    int writePos = 0;
//    final ByteBuffer keyBuffer;
//    final ByteBuffer dataBytesBuffer;


    public boolean hasNextSplit() {
        return writePos < recordLen;
    }

    public KvRecordSplit nextSplit(int splitSize) {
        int leftToWrite = recordLen - writePos;
        int canWriteLen = Math.min(splitSize, leftToWrite);
        ByteBuffer bb = ByteBuffer.allocate(canWriteLen);

        while (canWriteLen > 0) {
            bb.put(recordBytes[writePos]);
            writePos++;
            canWriteLen--;
        }
        return new KvRecordSplit(bb.array());
    }

    public int getLeftToWrite() {
        return recordLen - writePos;
    }

}
