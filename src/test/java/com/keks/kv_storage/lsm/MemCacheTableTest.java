package com.keks.kv_storage.lsm;

import com.keks.kv_storage.record.KVRecord;
import com.keks.kv_storage.lsm.query.SsTableRangeIterator;
import com.keks.kv_storage.lsm.query.LsmRecordsIterator;
import com.keks.kv_storage.query.range.MaxRangeKey;
import com.keks.kv_storage.query.range.MinRangeKey;
import com.keks.kv_storage.utils.UnCheckedConsumer;
import org.junit.jupiter.api.Test;
import utils.TestUtils;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;


class MemCacheTableTest {

    @Test
    public void testPutUpdateRemove1() {

        MemCacheTable memCacheTable = new MemCacheTable(4);
        assertTrue(memCacheTable.isEmpty());

        memCacheTable.remove("key10");
        assertEquals(1, memCacheTable.getApproxRecCnt());

        memCacheTable.remove("key11"); // 2 records in memory
        assertEquals(2, memCacheTable.getApproxRecCnt());

        memCacheTable.remove("key12"); // 3 records in memory
        assertEquals(3, memCacheTable.getApproxRecCnt());

        memCacheTable.remove("key13"); // 4 records in memory
        assertEquals(4, memCacheTable.getApproxRecCnt());

        memCacheTable.remove("key13"); // 4 records in memory
        assertEquals(4, memCacheTable.getApproxRecCnt());

        memCacheTable.put("key10", "10"); // 4 records in memory
        assertEquals(4, memCacheTable.getApproxRecCnt());

        memCacheTable.put("key11", "11"); // 4 records in memory
        assertEquals(4, memCacheTable.getApproxRecCnt());

        memCacheTable.put("key12", "12"); // 4 records in memory
        assertEquals(4, memCacheTable.getApproxRecCnt());

        memCacheTable.remove("key10"); // 4 records in memory
        assertEquals(4, memCacheTable.getApproxRecCnt());


        memCacheTable.remove("key11"); // 4 records in memory
        assertEquals(4, memCacheTable.getApproxRecCnt());

        memCacheTable.remove("key12"); // 4 records in memory
        assertEquals(4, memCacheTable.getApproxRecCnt());

        memCacheTable.remove("key13"); // 4 records in memory
        assertEquals(4, memCacheTable.getApproxRecCnt());


        memCacheTable.put("key9", "9"); // 5 records in memory
        assertEquals(5, memCacheTable.getApproxRecCnt());
        assertTrue(memCacheTable.isFull());

        memCacheTable.put("key9", "99"); // 5 records in memory
        assertEquals(5, memCacheTable.getApproxRecCnt());


        memCacheTable.remove("key10");
        assertEquals(5, memCacheTable.getApproxRecCnt());

        memCacheTable.remove("key11");
        assertEquals(5, memCacheTable.getApproxRecCnt());

        memCacheTable.remove("key12");
        assertEquals(5, memCacheTable.getApproxRecCnt());

        memCacheTable.remove("key13");
        assertEquals(5, memCacheTable.getApproxRecCnt());

        memCacheTable.remove("key9");
        assertEquals(5, memCacheTable.getApproxRecCnt());

        memCacheTable.put("key10", "a");
        memCacheTable.put("key11", "a");
        memCacheTable.put("key12", "a");
        memCacheTable.put("key13", "a");
        memCacheTable.put("key9", "a");
        assertEquals(5, memCacheTable.getApproxRecCnt());

        memCacheTable.addNewPartition();
        memCacheTable.dropOldestPartition();

        assertEquals(0, memCacheTable.getApproxRecCnt());
    }

    @Test
    public void testPutUpdateRemove2() throws Exception {
        int maxPartitionSize = 100;
        ReentrantLock lock = new ReentrantLock();
        MemCacheTable memCacheTable = new MemCacheTable(maxPartitionSize);

        UnCheckedConsumer<Integer, Exception> cons = i -> {
            try {

                String key = "key" + String.format("%05d", i);
                String value = "value" + i;
                memCacheTable.put(key, new KVRecord(key, value));
                if (i % 2 == 0) memCacheTable.remove(key);

                {
                    KVRecord kvRecord = memCacheTable.get(key);
                    if (i % 2 == 0) {
                        assertEquals(0, kvRecord.valueBytes.length);
                    } else {
                        assertEquals(key, kvRecord.key);
                    }
                }


                if (memCacheTable.isFull()) {
                    lock.lock();
                    if (memCacheTable.isFull()) {
                        memCacheTable.addNewPartition();
//                        droppedPartitions.addFirst(memCacheTable.getOldestPartition());
//                        memCacheTable.dropOldestPartition();
                    }
                    lock.unlock();
                }

                {
                    KVRecord kvRecord = memCacheTable.get(key);
                    if (i % 2 == 0) {
                        assertEquals(0, kvRecord.valueBytes.length);
                    } else {
                        assertEquals(key, kvRecord.key);
                    }
                }

                if (i % 10_000 == 0) {
                    System.out.println(i);
                }

            } catch (Throwable e) {
                System.out.println();
            }

        };


        TestUtils.runConcurrentTest(1_000_000, 1000, 100, cons, 50);

        assertTrue(memCacheTable.getApproxRecCnt() <= maxPartitionSize);
        System.out.println(memCacheTable.getApproxRecCnt());
        System.out.println(memCacheTable.getNewestPartition().getRecordsCnt());
        System.out.println(memCacheTable.getPartitionsCount());
    }

    @Test
    public void testRange1() {

        MemCacheTable memCacheTable = new MemCacheTable(100);

        for (int i = 10; i < 80; i++) {
            String key = String.format("%05d", i);
            String value = "value" + i;
            memCacheTable.put(key, new KVRecord(key, value));
        }

        {
            List<Iterator<KVRecord>> range = memCacheTable.getRangeParts("00012", "00015");
            Iterator<KVRecord> kvRecordIterator = range.get(0);
            LinkedList<KVRecord> records = new LinkedList<>();
            kvRecordIterator.forEachRemaining(records::add);

            assertEquals("00012", records.get(0).key);
            assertEquals("00015", records.get(records.size() - 1).key);
        }

        {
            List<Iterator<KVRecord>> range = memCacheTable.getRangeParts("00001", "00099");
            Iterator<KVRecord> kvRecordIterator = range.get(0);
            LinkedList<KVRecord> records = new LinkedList<>();
            kvRecordIterator.forEachRemaining(records::add);

            assertEquals("00010", records.get(0).key);
            assertEquals("00079", records.get(records.size() - 1).key);
        }

        {
            List<Iterator<KVRecord>> range = memCacheTable.getRangeToParts("00060");
            Iterator<KVRecord> kvRecordIterator = range.get(0);
            LinkedList<KVRecord> records = new LinkedList<>();
            kvRecordIterator.forEachRemaining(records::add);

            assertEquals("00010", records.get(0).key);
            assertEquals("00060", records.get(records.size() - 1).key);
        }

        {
            List<Iterator<KVRecord>> range = memCacheTable.getRangeFromParts("00020");
            Iterator<KVRecord> kvRecordIterator = range.get(0);
            LinkedList<KVRecord> records = new LinkedList<>();
            kvRecordIterator.forEachRemaining(records::add);

            assertEquals("00020", records.get(0).key);
            assertEquals("00079", records.get(records.size() - 1).key);
        }

    }

    @Test
    public void testRange2() throws Exception {
        int maxPartitionSize = 1000;
        ReentrantLock lock = new ReentrantLock();
        MemCacheTable memCacheTable = new MemCacheTable(maxPartitionSize);
        ConcurrentLinkedDeque<MemCachePartition> droppedPartitions = new ConcurrentLinkedDeque<>();

        UnCheckedConsumer<Integer, Exception> cons = i -> {
            try {

                String key = "key" + String.format("%05d", i);
                String value = "value" + i;
                memCacheTable.put(key, new KVRecord(key, value));
                if (i % 2 == 0) memCacheTable.remove(key);

                {
                    KVRecord kvRecord = memCacheTable.get(key);
                    if (kvRecord == null) {
                        for (MemCachePartition memCacheDroppedPartition : droppedPartitions) {
                            kvRecord = memCacheDroppedPartition.get(key);
                            if (kvRecord != null) break;
                        }
                    }
                    if (kvRecord == null) {
                        System.out.println();
                    }
                    if (i % 2 == 0) {
                        assertEquals(0, kvRecord.valueBytes.length);
                    } else {
                        assertEquals(key, kvRecord.key);
                    }
                }


                if (memCacheTable.isFull()) {
                    lock.lock();
                    if (memCacheTable.isFull()) {
                        memCacheTable.addNewPartition();
                        droppedPartitions.addFirst(memCacheTable.getOldestPartition());
//                        memCacheTable.dropOldestPartition();
                    }
                    lock.unlock();
                }

                {
                    KVRecord kvRecord = memCacheTable.get(key);
                    if (kvRecord == null) {
                        for (MemCachePartition memCacheDroppedPartition : droppedPartitions) {
                            kvRecord = memCacheDroppedPartition.get(key);
                            if (kvRecord != null) break;
                        }
                    }
                    if (kvRecord == null) {
                        System.out.println();
                    }
                    if (i % 2 == 0) {
                        assertEquals(0, kvRecord.valueBytes.length);
                    } else {
                        assertEquals(key, kvRecord.key);
                    }
                }

                {
                    if (i % 2 != 0) {
                        {
                            List<SsTableRangeIterator> rangeTo = memCacheTable
                                    .getRangeFromParts(key)
                                    .stream()
                                    .map(iter -> new SsTableRangeIterator(
                                            iter,
                                            new MinRangeKey(),
                                            new MaxRangeKey(), () ->{})
                                    ).collect(Collectors.toList());
                            LsmRecordsIterator recordsMergerIterator = new LsmRecordsIterator(rangeTo);
                            recordsMergerIterator.hasNext();
                            assertEquals(key, recordsMergerIterator.next().key);
                        }
                        {
                            List<SsTableRangeIterator> rangeTo = memCacheTable
                                    .getRangeToParts(key)
                                    .stream()
                                    .map(iter -> new SsTableRangeIterator(
                                            iter,
                                            new MinRangeKey(),
                                            new MaxRangeKey(), () ->{})
                                    ).collect(Collectors.toList());
                            LsmRecordsIterator recordsMergerIterator = new LsmRecordsIterator(rangeTo);
                            KVRecord last = null;
                            while (recordsMergerIterator.hasNext()) {
                                last = recordsMergerIterator.next();
                            }
                            assertEquals(key, last.key);
                        }
                    }
                }

                if (i % 10_000 == 0) {
                    System.out.println(i);
                }

            } catch (Throwable e) {
                throw e;
            }

        };


        TestUtils.runConcurrentTest(50_000, 1000, 100, cons, 50);

        assertTrue(memCacheTable.getApproxRecCnt() <= maxPartitionSize);
        System.out.println(memCacheTable.getApproxRecCnt());
        System.out.println(memCacheTable.getNewestPartition().getRecordsCnt());
        System.out.println(memCacheTable.getPartitionsCount());
    }

}