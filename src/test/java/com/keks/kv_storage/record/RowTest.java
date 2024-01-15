package com.keks.kv_storage.record;


import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;


class KvRowTest {

    @Test
    public void testRow1() {
        String key1 = "key1";
        String key2 = "123";
        String value1 = "value1";
        String value2 = "";
        String value3 = "avcsff";
        byte[][] key = new byte[2][];
        byte[][] value = new byte[3][];

        key[0] = key1.getBytes();
        key[1] = key2.getBytes();

        value[0] = value1.getBytes();
        value[1] = value2.getBytes();
        value[2] = value3.getBytes();
        KvRow kvRow = KvRow.of(key, value);

        {
            assertEquals(key1, new String(kvRow.keyBytes[0]));
            assertEquals(key2, new String(kvRow.keyBytes[1]));
            assertEquals(value1, new String(kvRow.valueBytes[0]));
            assertEquals(value2, new String(kvRow.valueBytes[1]));
            assertEquals(value3, new String(kvRow.valueBytes[2]));

            assertEquals(43, kvRow.getLen());
        }

        {
            ByteBuffer bb = ByteBuffer.wrap(kvRow.getBytes());
            KvRow kvRowAct = KvRow.fromByteBuffer(bb);
            assertEquals(key1, new String(kvRowAct.keyBytes[0]));
            assertEquals(key2, new String(kvRowAct.keyBytes[1]));
            assertEquals(value1, new String(kvRowAct.valueBytes[0]));
            assertEquals(value2, new String(kvRowAct.valueBytes[1]));
            assertEquals(value3, new String(kvRowAct.valueBytes[2]));
        }



    }

    @Test
    public void testRowIsDeleted1() {
        String key1 = "key1";
        String key2 = "123";
        byte[][] key = new byte[2][];
        byte[][] value = new byte[0][];

        key[0] = key1.getBytes();
        key[1] = key2.getBytes();


        KvRow kvRow = KvRow.of(key, value);

        {
            assertEquals(key1, new String(kvRow.keyBytes[0]));
            assertEquals(key2, new String(kvRow.keyBytes[1]));
            assertTrue(kvRow.isDeleted());
            assertEquals(19, kvRow.getLen());
        }

        {
            ByteBuffer bb = ByteBuffer.wrap(kvRow.getBytes());
            KvRow kvRowAct = KvRow.fromByteBuffer(bb);
            assertEquals(key1, new String(kvRowAct.keyBytes[0]));
            assertEquals(key2, new String(kvRowAct.keyBytes[1]));
            assertTrue(kvRow.isDeleted());
            assertEquals(19, kvRow.getLen());
        }
    }

}