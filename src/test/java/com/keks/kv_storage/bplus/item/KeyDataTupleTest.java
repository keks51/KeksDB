package com.keks.kv_storage.bplus.item;

import com.keks.kv_storage.record.KVRecord;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;


class KeyDataTupleTest {

    @Test
    public void test1() {
        String key = "key1";
        String value = "value1";

        KVRecord recordToSplit = new KVRecord(key, value.getBytes());
        int tupleLen = recordToSplit.getLen();

        KVRecordSplitter splitter = new KVRecordSplitter(recordToSplit);
        KvRecordSplit kvRecordSplit = splitter.nextSplit(200);

        assertFalse(splitter.hasNextSplit());

        KVRecordSplitsBuilder recordSplitsBuilder = new KVRecordSplitsBuilder(tupleLen);
        recordSplitsBuilder.add(ByteBuffer.wrap(kvRecordSplit.getBytes()));


        KVRecord keyDataTupleMerged = recordSplitsBuilder.buildKVRecord();

        assertEquals(key, keyDataTupleMerged.key);
        assertEquals(value, new String(keyDataTupleMerged.valueBytes));
    }

    @Test
    public void test2() {
        String key = "key111";
        String value50 = "Lorem ipsum dolor sit amet, consectetur adipiscing";

        KVRecord recordToSplit = new KVRecord(key, value50.getBytes());
        int tupleLen = recordToSplit.getLen();

        KVRecordSplitter splitter = new KVRecordSplitter(recordToSplit);
        KvRecordSplit block1 = splitter.nextSplit(50);
        KvRecordSplit block2 = splitter.nextSplit(50);

        assertFalse(splitter.hasNextSplit());

        KVRecordSplitsBuilder recordSplitsBuilder = new KVRecordSplitsBuilder(tupleLen);
        recordSplitsBuilder.add(ByteBuffer.wrap(block1.getBytes()));
        recordSplitsBuilder.add(ByteBuffer.wrap(block2.getBytes()));

        KVRecord keyDataTupleMerged = recordSplitsBuilder.buildKVRecord();

        assertEquals(key, keyDataTupleMerged.key);
        assertEquals(value50, new String(keyDataTupleMerged.valueBytes));
    }

    @Test
    public void test4() {
        String key = "key1";
        String value = "";

        KVRecord recordToSplit = new KVRecord(key, value.getBytes());
        int tupleLen = recordToSplit.getLen();

        KVRecordSplitter splitter = new KVRecordSplitter(recordToSplit);
        KvRecordSplit block = splitter.nextSplit(200);

        assertFalse(splitter.hasNextSplit());

        KVRecordSplitsBuilder recordSplitsBuilder = new KVRecordSplitsBuilder(tupleLen);
        recordSplitsBuilder.add(ByteBuffer.wrap(block.getBytes()));
        KVRecord keyDataTupleMerged = recordSplitsBuilder.buildKVRecord();

        assertEquals(key, keyDataTupleMerged.key);
        assertEquals(value, new String(keyDataTupleMerged.valueBytes));
    }

    @Test
    public void test5() {
        String key = "key1";
        String value700Len = "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Phasellus rhoncus elit quis " +
                "augue mollis, eget interdum augue ornare. Sed sit amet orci at arcu pulvinar tincidunt. Cras in sem " +
                "nibh. Mauris facilisis varius congue. Curabitur placerat ut nibh dictum ultricies. Class aptent " +
                "taciti sociosqu ad litora torquent per conubia nostra, per inceptos himenaeos. Aliquam semper erat " +
                "sed ex finibus, sodales tempus massa sodales. Sed dictum mattis tellus non mollis. Curabitur " +
                "imperdiet feugiat risus nec tempus. Nullam porta nulla quis ligula aliquet faucibus. Praesent " +
                "convallis eleifend est, eu facilisis ante. Sed cursus commodo sapien, vitae condimentum lacus " +
                "convallis vel. Curabitur ut nibh varius";

        KVRecord recordToSplit = new KVRecord(key, value700Len.getBytes());
        int tupleLen = recordToSplit.getLen();
        ArrayList<KvRecordSplit> blocks = new ArrayList<>();
        KVRecordSplitter splitter = new KVRecordSplitter(recordToSplit);
        int pageSize = 50;
        do {
            KvRecordSplit block = splitter.nextSplit(pageSize);
            pageSize = pageSize + 50;
            blocks.add(block);
        } while (splitter.hasNextSplit());

        KVRecordSplitsBuilder recordSplitsBuilder = new KVRecordSplitsBuilder(tupleLen);
        blocks.forEach(e -> recordSplitsBuilder.add(ByteBuffer.wrap(e.getBytes())));

        KVRecord keyDataTuple = recordSplitsBuilder.buildKVRecord();


        assertEquals(key, keyDataTuple.key);
        assertEquals(value700Len, new String(keyDataTuple.valueBytes));
    }

    @Test
    public void test6() {

        String key700Len = "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Phasellus rhoncus elit quis " +
                "augue mollis, eget interdum augue ornare. Sed sit amet orci at arcu pulvinar tincidunt. Cras in sem " +
                "nibh. Mauris facilisis varius congue. Curabitur placerat ut nibh dictum ultricies. Class aptent " +
                "taciti sociosqu ad litora torquent per conubia nostra, per inceptos himenaeos. Aliquam semper erat " +
                "sed ex finibus, sodales tempus massa sodales. Sed dictum mattis tellus non mollis. Curabitur " +
                "imperdiet feugiat risus nec tempus. Nullam porta nulla quis ligula aliquet faucibus. Praesent " +
                "convallis eleifend est, eu facilisis ante. Sed cursus commodo sapien, vitae condimentum lacus " +
                "convallis vel. Curabitur ut nibh varius";

        String value = "value1";

        KVRecord recordToSplit = new KVRecord(key700Len, value.getBytes());
        int tupleLen = recordToSplit.getLen();
        ArrayList<KvRecordSplit> blocks = new ArrayList<>();
        KVRecordSplitter splitter = new KVRecordSplitter(recordToSplit);
        int pageSize = 50;
        do {
            KvRecordSplit block = splitter.nextSplit(pageSize);
            pageSize = pageSize + 50;
            blocks.add(block);
        } while (splitter.hasNextSplit());

        KVRecordSplitsBuilder recordSplitsBuilder = new KVRecordSplitsBuilder(tupleLen);
        blocks.forEach(e -> recordSplitsBuilder.add(ByteBuffer.wrap(e.getBytes())));

        KVRecord keyDataTuple = recordSplitsBuilder.buildKVRecord();


        assertEquals(key700Len, keyDataTuple.key);
        assertEquals(value, new String(keyDataTuple.valueBytes));
    }

}