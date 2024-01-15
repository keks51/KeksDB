package com.keks.kv_storage.lsm.ss_table;

import com.keks.kv_storage.record.KVRecord;
import com.keks.kv_storage.lsm.conf.LsmConf;
import com.keks.kv_storage.lsm.io.BloomFilterRafRW;
import com.keks.kv_storage.lsm.io.MetadataJsonRW;
import com.keks.kv_storage.lsm.io.SSTableReader;
import com.keks.kv_storage.lsm.io.SSTableWriter;
import com.keks.kv_storage.lsm.utils.BloomFilter;
import com.keks.kv_storage.query.range.RangeSearchKey;
import com.keks.kv_storage.utils.UnCheckedConsumer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import utils.TestUtils;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.keks.kv_storage.lsm.io.SSTableReaderTest.createSSTable;
import static org.junit.jupiter.api.Assertions.*;


class SSTableTest {

    @Test
    public void testSSTableSearchKey(@TempDir Path tmpPath) throws IOException {
        LsmConf lsmConf = new LsmConf(3, 10, 0.5);
        SSTableWriter ssTableWriter = new SSTableWriter(tmpPath.toFile(), lsmConf, 20);
        ArrayList<KVRecord> expKVRecords = new ArrayList<>(List.of(
                new KVRecord("k1", "v1"), new KVRecord("k2", "v2"), new KVRecord("k3", ""),
                new KVRecord("k4", "v1"), new KVRecord("k5", "")));

        ssTableWriter.createSSTable(expKVRecords.iterator());

        SSTableReader ssTableReader = new SSTableReader(tmpPath.toFile(), 3);
        SSTableMetadata ssTableMetadata = new MetadataJsonRW(tmpPath.toFile()).read();
        BloomFilter bloomFilter = new BloomFilterRafRW(tmpPath.toFile()).read();
        SSTable ssTable = new SSTable(1, 1, tmpPath.toFile(), ssTableReader, ssTableMetadata, bloomFilter);
        for (KVRecord expKVRecord : expKVRecords) {
            KVRecord resRecord = ssTable.searchKey(expKVRecord.key);
            assertEquals(expKVRecord, resRecord);
        }
        assertNull(ssTable.searchKey("k0"));
        assertNull(ssTable.searchKey("k6"));
    }

    @Test
    public void testSSTableReadAllRecords(@TempDir Path tmpPath) throws IOException {
        ArrayList<KVRecord> expKVRecords = new ArrayList<>(List.of(
                new KVRecord("k1", "v1"), new KVRecord("k2", "v2"), new KVRecord("k3", ""),
                new KVRecord("k4", "v1"), new KVRecord("k5", "")));
        {
            LsmConf lsmConf = new LsmConf(3, 10, 0.5);
            SSTableWriter ssTableWriter = new SSTableWriter(tmpPath.toFile(), lsmConf, 20);
            ssTableWriter.createSSTable(expKVRecords.iterator());
        }

        {
            SSTableReader ssTableReader = new SSTableReader(tmpPath.toFile(), 3);
            SSTableMetadata ssTableMetadata = new MetadataJsonRW(tmpPath.toFile()).read();
            BloomFilter bloomFilter = new BloomFilterRafRW(tmpPath.toFile()).read();
            SSTable ssTable = new SSTable(1, 1, tmpPath.toFile(), ssTableReader, ssTableMetadata, bloomFilter);
            Iterator<KVRecord> resIter = ssTable.readAllRecords();
            List<KVRecord> resKVRecords = new ArrayList<>();
            while (resIter.hasNext()) resKVRecords.add(resIter.next());
            assertIterableEquals(expKVRecords, resKVRecords);
        }
    }

    @Test
    public void testSearchKeys(@TempDir Path tmpPath) throws IOException {
        ArrayList<KVRecord> expKVRecords = new ArrayList<>(List.of(
                new KVRecord("k02", "v02"),
                new KVRecord("k05", ""),
                new KVRecord("k07", "v07"),
                new KVRecord("k09", ""),
                new KVRecord("k11", "v11"),
                new KVRecord("k13", "v13")));
        {
            LsmConf lsmConf = new LsmConf(3, 10, 0.5);
            SSTableWriter ssTableWriter = new SSTableWriter(tmpPath.toFile(), lsmConf, 20);
            ssTableWriter.createSSTable(expKVRecords.iterator());
        }

        {
            SSTableReader ssTableReader = new SSTableReader(tmpPath.toFile(), 3);
            SSTableMetadata ssTableMetadata = new MetadataJsonRW(tmpPath.toFile()).read();
            BloomFilter bloomFilter = new BloomFilterRafRW(tmpPath.toFile()).read();
            SSTable ssTable = new SSTable(1, 1, tmpPath.toFile(), ssTableReader, ssTableMetadata, bloomFilter);

            {
                Iterator<KVRecord> kvRecordIterator = ssTable
                        .searchRange(new RangeSearchKey("k00", true), new RangeSearchKey("k01", true));
                assertFalse(kvRecordIterator.hasNext());
            }

            {
                Iterator<KVRecord> kvRecordIterator = ssTable
                        .searchRange(new RangeSearchKey("k00", true), new RangeSearchKey("k08", true));
                ArrayList<KVRecord> exp = new ArrayList<>() {{
                    add(new KVRecord("k02", "v02"));
                    add(new KVRecord("k05", ""));
                    add(new KVRecord("k07", "v07"));
                }};
                TestUtils.iterateAndAssert(exp.iterator(), kvRecordIterator);
            }

            {
                Iterator<KVRecord> kvRecordIterator = ssTable
                        .searchRange(new RangeSearchKey("k08", true), new RangeSearchKey("k17", true));
                ArrayList<KVRecord> exp = new ArrayList<>() {{
                    add(new KVRecord("k09", ""));
                    add(new KVRecord("k11", "v11"));
                    add(new KVRecord("k13", "v13"));
                }};
                TestUtils.iterateAndAssert(exp.iterator(), kvRecordIterator);
            }

            {
                Iterator<KVRecord> kvRecordIterator = ssTable
                        .searchRange(new RangeSearchKey("k14", true), new RangeSearchKey("k15", true));
                assertFalse(kvRecordIterator.hasNext());
            }

            {
                Iterator<KVRecord> kvRecordIterator = ssTable
                        .searchRange(new RangeSearchKey("k06", true), new RangeSearchKey("k12", true));
                ArrayList<KVRecord> exp = new ArrayList<>() {{
                    add(new KVRecord("k07", "v07"));
                    add(new KVRecord("k09", ""));
                    add(new KVRecord("k11", "v11"));
                }};
                TestUtils.iterateAndAssert(exp.iterator(), kvRecordIterator);
            }

            {
                Iterator<KVRecord> kvRecordIterator = ssTable
                        .searchRange(new RangeSearchKey("k00", true), new RangeSearchKey("k20", true));
                ArrayList<KVRecord> exp = new ArrayList<>() {{
                    add(new KVRecord("k02", "v02"));
                    add(new KVRecord("k05", ""));
                    add(new KVRecord("k07", "v07"));
                    add(new KVRecord("k09", ""));
                    add(new KVRecord("k11", "v11"));
                    add(new KVRecord("k13", "v13"));
                }};
                TestUtils.iterateAndAssert(exp.iterator(), kvRecordIterator);
            }
        }
    }

    @Test
    public void testSearchKeys2(@TempDir Path tmpPath) throws IOException {
        int numberOfKeysInIndex = 5;
        int minRecNum = 200;
        int maxRecNum = 800;
        ArrayList<KVRecord> allRecords = new ArrayList<>();
        ArrayList<KVRecord> recordsToSave = new ArrayList<>();

        {
            for (int i = 0; i < minRecNum; i++) {
                allRecords.add(null);
            }
            for (int i = minRecNum; i < maxRecNum; i++) {
                if (i % 5 == 0) {
                    KVRecord kvRecord = new KVRecord("key" + String.format("%07d", i), ("value" + i).getBytes());
                    recordsToSave.add(kvRecord);
                    allRecords.add(kvRecord);
                } else {
                    allRecords.add(null);
                }
            }
            for (int i = maxRecNum; i < maxRecNum + 200; i++) {
                allRecords.add(null);
            }

            LsmConf lsmConf = new LsmConf(numberOfKeysInIndex, 10, 0.5);
            createSSTable(tmpPath, lsmConf, recordsToSave);
        }

        {
            SSTableReader ssTableReader = new SSTableReader(tmpPath.toFile(), numberOfKeysInIndex);
            SSTableMetadata ssTableMetadata = new MetadataJsonRW(tmpPath.toFile()).read();
            BloomFilter bloomFilter = new BloomFilterRafRW(tmpPath.toFile()).read();
            SSTable ssTable = new SSTable(1, 1, tmpPath.toFile(), ssTableReader, ssTableMetadata, bloomFilter);

            for (int l = 0; l < maxRecNum + 200; l = l + 13) {
                String leftKey = "key" + String.format("%07d", l);
                for (int r = l + 50; r < maxRecNum + 200; r = r + 13) {
                    String rightKey = "key" + String.format("%07d", r);
                    List<KVRecord> kvRecords = allRecords.subList(l, r + 1);
                    List<KVRecord> exp = kvRecords.stream().filter(Objects::nonNull).collect(Collectors.toList());
                    Iterator<KVRecord> kvRecordIterator = ssTable
                            .searchRange(new RangeSearchKey(leftKey, true), new RangeSearchKey(rightKey, true));

                    TestUtils.iterateAndAssert(exp.iterator(), kvRecordIterator);
                }
            }

        }

    }

    @Test
    public void testSearchKeys2Concurrent(@TempDir Path tmpPath) throws IOException, ExecutionException, InterruptedException, TimeoutException {
        int numberOfKeysInIndex = 5;
        int minRecNum = 200;
        int maxRecNum = 800;
        ArrayList<KVRecord> allRecords = new ArrayList<>();
        ArrayList<KVRecord> recordsToSave = new ArrayList<>();

        {
            for (int i = 0; i < minRecNum; i++) {
                allRecords.add(null);
            }
            for (int i = minRecNum; i < maxRecNum; i++) {
                if (i % 5 == 0) {
                    KVRecord kvRecord = new KVRecord("key" + String.format("%07d", i), ("value" + i).getBytes());
                    recordsToSave.add(kvRecord);
                    allRecords.add(kvRecord);
                } else {
                    allRecords.add(null);
                }
            }
            for (int i = maxRecNum; i < maxRecNum + 200; i++) {
                allRecords.add(null);
            }

            LsmConf lsmConf = new LsmConf(numberOfKeysInIndex, 10, 0.5);
            createSSTable(tmpPath, lsmConf, recordsToSave);
        }

        {
            SSTableReader ssTableReader = new SSTableReader(tmpPath.toFile(), numberOfKeysInIndex);
            SSTableMetadata ssTableMetadata = new MetadataJsonRW(tmpPath.toFile()).read();
            BloomFilter bloomFilter = new BloomFilterRafRW(tmpPath.toFile()).read();
            SSTable ssTable = new SSTable(1, 1, tmpPath.toFile(), ssTableReader, ssTableMetadata, bloomFilter);

            UnCheckedConsumer<Integer, IOException> func = index -> {
                for (int l = 0; l < maxRecNum + 200; l = l + 13) {
                    String leftKey = "key" + String.format("%07d", l);
                    for (int r = l + 50; r < maxRecNum + 200; r = r + 13) {
                        String rightKey = "key" + String.format("%07d", r);
                        List<KVRecord> kvRecords = allRecords.subList(l, r + 1);
                        List<KVRecord> exp = kvRecords.stream().filter(Objects::nonNull).collect(Collectors.toList());
                        Iterator<KVRecord> kvRecordIterator = ssTable
                                .searchRange(new RangeSearchKey(leftKey, true), new RangeSearchKey(rightKey, true));

                        TestUtils.iterateAndAssert(exp.iterator(), kvRecordIterator);
                    }
                }
            };
            runConcurrentTest(100, 1000, 10, func, 200);

        }

    }

    @Test
    public void testSearchKeyConcurrent(@TempDir Path tmpPath) throws IOException, ExecutionException, InterruptedException, TimeoutException {
        int numberOfKeysInIndex = 13;
        LsmConf lsmConf = new LsmConf(numberOfKeysInIndex, 10, 0.5);
        int rec = 1_000_000;
        ArrayList<KVRecord> records = new ArrayList<>(rec);
        for (int i = 0; i < rec; i = i + 2) {
            records.add(new KVRecord("key" + String.format("%07d", i), ("value" + i).getBytes()));
        }
        createSSTable(tmpPath, lsmConf, records);


        SSTableReader ssTableReader = new SSTableReader(tmpPath.toFile(), numberOfKeysInIndex);
        SSTableMetadata ssTableMetadata = new MetadataJsonRW(tmpPath.toFile()).read();
        BloomFilter bloomFilter = new BloomFilterRafRW(tmpPath.toFile()).read();
        SSTable ssTable = new SSTable(1, 1, tmpPath.toFile(), ssTableReader, ssTableMetadata, bloomFilter);

        AtomicInteger cnt = new AtomicInteger();
        UnCheckedConsumer<Integer, IOException> func = index -> {

            KVRecord resRecord = ssTable.searchKey("key" + String.format("%07d", index));
            if (index % 2 == 0) {
                KVRecord expKVRecord = records.get(index / 2);
                assertEquals(expKVRecord, resRecord);
            } else {
                assertNull(resRecord);
            }


            int i = cnt.incrementAndGet();
            if (i % 100_000 == 0) {
//                System.out.println(i);
            }

        };

        Instant start = Instant.now();
        runConcurrentTest(rec, 1000, 10, func, 200);
        Instant finish = Instant.now();
        Duration between = Duration.between(start, finish);
//        System.out.println(between);
    }

    public static <EX extends Exception> void runConcurrentTest(int taskCount,
                                                                int threadPoolAwaitTimeoutSec,
                                                                int taskAwaitTimeoutSec,
                                                                UnCheckedConsumer<Integer, EX> function,
                                                                int numberOfThreads) throws EX, InterruptedException, ExecutionException, TimeoutException {
        ExecutorService executor = Executors.newFixedThreadPool(numberOfThreads);


        List<Future<?>> futures = new ArrayList<>(taskCount);
        for (int i = 0; i < taskCount; i++) {
            int y = i;
            Future<?> future1 = executor.submit(() -> {
                try {
                    function.accept(y);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
            futures.add(future1);
        }

        executor.shutdown();
        if (!executor.awaitTermination(threadPoolAwaitTimeoutSec, TimeUnit.SECONDS)) {
            throw new InterruptedException();
        }

        for (Future<?> future : futures) {
            future.get(taskAwaitTimeoutSec, TimeUnit.SECONDS);
//            future.get();
        }

    }

}