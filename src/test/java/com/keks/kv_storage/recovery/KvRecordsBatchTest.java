package com.keks.kv_storage.recovery;

import com.keks.kv_storage.record.KVRecord;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;

import static org.junit.jupiter.api.Assertions.assertEquals;


class KvRecordsBatchTest {

    @Test
    public void test1() {
        ArrayList<KVRecord> kvRecords = new ArrayList<>();
        int records = 50;
        int bytesLen = 0;

        for (int i = 0; i < records; i++) {
            String key = "key" + i;
            String value = "value" + i;

            KVRecord kvRecord = new KVRecord(key, value);
            bytesLen += kvRecord.getLen();
            kvRecords.add(kvRecord);
        }
        KvRecordsBatch kvRecordsBatch = new KvRecordsBatch(1, kvRecords, bytesLen);
        byte[] bytes = kvRecordsBatch.getBytes();
        System.out.println(bytesLen);


        KvRecordsBatch kvRecordsBatchAct = KvRecordsBatch.fromByteBuffer(ByteBuffer.wrap(bytes));
        assertEquals(kvRecords, kvRecordsBatchAct.kvRecords);
        assertEquals(kvRecordsBatch.getLen(),kvRecordsBatchAct.getLen());
        assertEquals(kvRecordsBatch.batchId,kvRecordsBatchAct.batchId);
    }

}
