package com.keks.kv_storage.record;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.*;


class KVRecordTest {



    @Test
    public void test2() {
        String key = "key1";

        KVRecord kvRecord = new KVRecord(key);

        {
            assertEquals(key, kvRecord.key);
            assertEquals("", new String(kvRecord.valueBytes));
            assertEquals(18, kvRecord.getLen());
            assertTrue(kvRecord.isDeleted());
        }

        {
            assertEquals(key, kvRecord.key);
            assertTrue(kvRecord.isDeleted());
            assertEquals(18, kvRecord.getLen());
        }
    }

    @Test
    public void test3() {
        String key = "key1";
        String value = "value1";

        KVRecord kvRecord = new KVRecord(key, value);

        {
            assertEquals(key, kvRecord.key);
            assertEquals(value, new String(kvRecord.valueBytes));
            assertFalse(kvRecord.isDeleted());
            assertEquals(24, kvRecord.getLen());
        }

        {
            byte[] bytes = kvRecord.getBytes();
            KvRow kvRow = KvRow.fromByteBuffer(ByteBuffer.wrap(bytes));
            KVRecord actKvRec = new KVRecord(kvRow.keyBytes[0], kvRow.valueBytes[0]);
            assertEquals(key, actKvRec.key);
            assertEquals(value, new String(actKvRec.valueBytes));
            assertEquals(24, actKvRec.getLen());
            assertFalse(kvRecord.isDeleted());
        }
    }

}