package com.keks.kv_storage.recovery;

import com.keks.kv_storage.Item;
import com.keks.kv_storage.TypeSize;
import com.keks.kv_storage.record.KVRecord;
import com.keks.kv_storage.record.KvRow;

import java.nio.ByteBuffer;
import java.util.ArrayList;


public class KvRecordsBatch implements Item {


    public final int batchId;
    public final ArrayList<KVRecord> kvRecords;

    public final int totalLen;

    public KvRecordsBatch(int batchId, ArrayList<KVRecord> kvRecords, int size) {
        this.batchId = batchId;
        this.kvRecords = kvRecords;
        this.totalLen = TypeSize.INT + TypeSize.INT + size + TypeSize.INT; // total_len, seq_id, kv_records_bytes,total_len_for_check
    }

    public static KvRecordsBatch fromByteBuffer(ByteBuffer bb) {
        long startPos = bb.position();
        int totalLen = bb.getInt();
        if (bb.remaining() + TypeSize.INT < totalLen) throw new RuntimeException();

        int batchId = bb.getInt();
        ArrayList<KVRecord> kvRecords = new ArrayList<>();
        while (bb.position() + TypeSize.INT < startPos + totalLen) {
            KvRow kvRow = KVRecord.fromByteBuffer(bb);
            KVRecord kvRecord = new KVRecord(kvRow.keyBytes[0], kvRow.valueBytes[0]);
            kvRecords.add(kvRecord);
        }
        int size = (int) (bb.position() - (startPos + TypeSize.INT + TypeSize.INT)); // bb.pos - (total_len(int) + batch_id(int))
        assert totalLen == bb.getInt();
        return new KvRecordsBatch(batchId,  kvRecords, size);
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
        bb.putInt(batchId);
        for (KVRecord kvRecord : kvRecords) {
            bb.put(kvRecord.getBytes());
        }
        bb.putInt(totalLen);
    }

    @Override
    public byte[] getBytes() {
        ByteBuffer bb = ByteBuffer.allocate(totalLen);
        copyToBB(bb);
        return bb.array();
    }

}
