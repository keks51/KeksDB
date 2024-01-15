package com.keks.kv_storage.lsm.io;

import com.keks.kv_storage.record.KVRecord;
import com.keks.kv_storage.lsm.conf.LsmConf;
import com.keks.kv_storage.lsm.ss_table.IndexedKey;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Consumer;

import static com.keks.kv_storage.lsm.io.SSTableWriter.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;


public class SSTableReaderTest {

    @Test
    public void testReadPartialIndex(@TempDir Path dir) throws IOException {
        int numberOfKeysInIndex = 3;
        ArrayList<KVRecord> expPartialIdxList = new ArrayList<>();
        {
            LsmConf lsmConf = new LsmConf(numberOfKeysInIndex, 10, 0.5);
            int rec = 10_000;
            ArrayList<KVRecord> allRecordsList = new ArrayList<>(rec);
            for (int i = 0; i < rec; i++) {
                KVRecord kvRecord = new KVRecord("key" + String.format("%05d", i), ("value" + i).getBytes());
                allRecordsList.add(kvRecord);
                if (i % numberOfKeysInIndex == 0) {
                    expPartialIdxList.add(kvRecord);
                }

            }
            createSSTable(dir, lsmConf, allRecordsList);
        }

        {
            SSTableReader ssTableReader = new SSTableReader(dir.toFile(), numberOfKeysInIndex);
            ArrayList<IndexedKey> actPartialIndexes = ssTableReader.readSparseIndex();
            for (int i = 0; i < actPartialIndexes.size(); i++) {
                assertEquals(actPartialIndexes.get(i).key, expPartialIdxList.get(i).key);
            }
        }

    }

    @Test
    public void testReadIndex(@TempDir Path dir) throws IOException {
        int numberOfKeysInIndex = 13;
        LsmConf lsmConf = new LsmConf(numberOfKeysInIndex, 10, 0.5);
        int rec = 10_000;
        ArrayList<KVRecord> records = new ArrayList<>(rec);
        for (int i = 0; i < rec; i++) {
            records.add(new KVRecord("key" + i, ("value" + i).getBytes()));
        }
        createSSTable(dir, lsmConf, records);

        {
            FileChannel indexChannel = FileChannel.open(new File(dir.toFile(), DENSE_INDEX_FILE_NAME).toPath(), StandardOpenOption.READ);
            SSTableReader.DenseIndexReader denseIndexReader = new SSTableReader.DenseIndexReader(indexChannel, rec, 0, 32 * 1024);

            int cnt = 0;
            while (denseIndexReader.hasNext()) {
                IndexedKey next = denseIndexReader.next();
                assertEquals(records.get(cnt).key, next.key);
                cnt++;
            }
        }

        {
            SSTableReader ssTableReader = new SSTableReader(dir.toFile(), numberOfKeysInIndex);
            ArrayList<IndexedKey> partialIndexes = ssTableReader.readSparseIndex();
            FileChannel indexChannel = FileChannel.open(new File(dir.toFile(), DENSE_INDEX_FILE_NAME).toPath(), StandardOpenOption.READ);
            for (int i = 0; i < partialIndexes.size(); i++) {
                IndexedKey partialIndex = partialIndexes.get(i);
                long blockStartPos = partialIndex.posInReferenceFile;
                SSTableReader.DenseIndexReader denseIndexReader = new SSTableReader.DenseIndexReader(indexChannel, numberOfKeysInIndex, blockStartPos, 32 * 1024);
                int cnt = 0;
                while (denseIndexReader.hasNext()) {
                    denseIndexReader.next();
                    cnt++;
                }
                if (i == partialIndexes.size() - 2) { // only works when rec = 10_000;
                    assertEquals(rec - numberOfKeysInIndex * (rec / numberOfKeysInIndex), cnt);
                } else if (i == partialIndexes.size() - 1) { // only works when rec = 10_000;
                    assertEquals(1, cnt); // maxKey
                } else {
                    if (numberOfKeysInIndex != cnt) {
                        assertEquals(numberOfKeysInIndex, cnt);
                    }

                }

            }

        }
    }

    @Test
    public void testReadData(@TempDir Path dir) throws IOException {
        LsmConf lsmConf = new LsmConf(3, 10, 0.5);
        int rec = 10_000;
        ArrayList<KVRecord> records = new ArrayList<>(rec);
        for (int i = 0; i < rec; i++) {
            records.add(new KVRecord("key" + i, ("value" + i).getBytes()));
        }
        createSSTable(dir, lsmConf, records);


        {
            FileChannel dataChannel = FileChannel.open(new File(dir.toFile(), DATA_FILE_NAME).toPath(), StandardOpenOption.READ);
            SSTableReader.DataReader dataReader = new SSTableReader.DataReader(dataChannel, 0, 32 * 1024);

            int cnt = 0;
            while (dataReader.hasNext()) {
                KVRecord actKVRecord = dataReader.next();
                assertEquals(records.get(cnt), actKVRecord);
                cnt++;
            }
        }
    }

    @Test
    public void testReadDataContainingHugeRecords(@TempDir Path dir) throws IOException {
        LsmConf lsmConf = new LsmConf(3, 10, 0.5);
        int rec = 1_000_000;
        ArrayList<KVRecord> records = new ArrayList<>(rec);
        for (int i = 0; i < rec; i++) {
            if (i % 100 == 0) {
                records.add(new KVRecord("key" + i, ("value".repeat(new Random().nextInt(20)) + i).getBytes()));
            } else {
                records.add(new KVRecord("key" + i, ("value" + i).getBytes()));
            }

        }
        createSSTable(dir, lsmConf, records);

        {
            FileChannel dataChannel = FileChannel.open(new File(dir.toFile(), DATA_FILE_NAME).toPath(), StandardOpenOption.READ);
            SSTableReader.DataReader dataReader = new SSTableReader.DataReader(dataChannel, 0, 30);

            int cnt = 0;
            while (dataReader.hasNext()) {
                KVRecord actKVRecord = dataReader.next();
                assertEquals(records.get(cnt), actKVRecord);
                cnt++;
            }
        }
    }

    @Test
    public void testReadAllRecordsByIterator(@TempDir Path dir) throws IOException {
        LsmConf lsmConf = new LsmConf(3, 10, 0.5);
        int rec = 10_000;
        ArrayList<KVRecord> records = new ArrayList<>(rec);
        for (int i = 0; i < rec; i++) {
            records.add(new KVRecord("key" + i, ("value" + i).getBytes()));
        }
        createSSTable(dir, lsmConf, records);

        {
            SSTableReader ssTableReader = new SSTableReader(dir.toFile(), 3);
            Iterator<KVRecord> KVRecordIterator = ssTableReader.readRecords(0);
            int cnt = 0;
            while (KVRecordIterator.hasNext()) {
                KVRecord actKVRecord = KVRecordIterator.next();
                assertEquals(records.get(cnt), actKVRecord);
                cnt++;
            }
        }
    }

    @Test
    public void testFindKeyInIndexBlock(@TempDir Path dir) throws IOException, ExecutionException, InterruptedException, TimeoutException {
        int numberOfKeysInIndex = 13;
        LsmConf lsmConf = new LsmConf(numberOfKeysInIndex, 10, 0.5);
        int rec = 1_000;
        ArrayList<KVRecord> records = new ArrayList<>(rec);
        for (int i = 0; i < rec; i++) {
            records.add(new KVRecord("key" + String.format("%05d", i), ("value" + i).getBytes()));
        }
        createSSTable(dir, lsmConf, records);

        SSTableReader ssTableReader = new SSTableReader(dir.toFile(), numberOfKeysInIndex);
        ArrayList<IndexedKey> partialIndexedKeys = ssTableReader.readSparseIndex();

        Consumer<Integer> func = index -> {
            try {
                IndexedKey partialIndex = partialIndexedKeys.get(index);
                long indexBlockStartPos = partialIndex.posInReferenceFile;
                for (int i = 0; i < rec; i++) {
                    String expKey = "key" + String.format("%05d", i);
                    IndexedKey keyInIndexBlock;
                    keyInIndexBlock = ssTableReader.findKeyInDenseIndex(expKey, indexBlockStartPos);
                    int leftBound = index * numberOfKeysInIndex;
                    int rightBound = leftBound + numberOfKeysInIndex;
                    if (leftBound <= i && i < rightBound || (i == rec - 1 && i == leftBound - 2)) { // right condition is for maxKey
                        assertEquals(expKey, keyInIndexBlock.key);
                    } else {
                        if (keyInIndexBlock != null) {
                            System.out.println();
                        }
                        assertNull(keyInIndexBlock);
                    }

                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        };
        runConcurrentTest(partialIndexedKeys.size(), 1000, 10, func, 10);
    }

    @Test
    public void testReadKVRecord(@TempDir Path dir) throws IOException {
        LsmConf lsmConf = new LsmConf(13, 10, 0.5);
        int rec = 10_000;
        ArrayList<KVRecord> records = new ArrayList<>(rec);
        for (int i = 0; i < rec; i++) {
            records.add(new KVRecord("key" + i, ("value" + i).getBytes()));
        }
        createSSTable(dir, lsmConf, records);

        {
            FileChannel indexChannel = FileChannel.open(new File(dir.toFile(), DENSE_INDEX_FILE_NAME).toPath(), StandardOpenOption.READ);
            SSTableReader.DenseIndexReader denseIndexReader = new SSTableReader.DenseIndexReader(indexChannel, rec, 0, 32 * 1024);
            SSTableReader ssTableReader = new SSTableReader(dir.toFile(), 13);

            int cnt = 0;
            while (denseIndexReader.hasNext()) {
                IndexedKey next = denseIndexReader.next();
                KVRecord expKVRecord = records.get(cnt);
                KVRecord KVRecord = ssTableReader.readKVRecord(next.posInReferenceFile, next.refRecordLen);
                assertEquals(expKVRecord, KVRecord);
                cnt++;
            }
        }

    }


    public static void runConcurrentTest(int taskCount,
                                         int threadPoolAwaitTimeoutSec,
                                         int taskAwaitTimeoutSec,
                                         Consumer<Integer> function,
                                         int numberOfThreads) throws InterruptedException, ExecutionException, TimeoutException {
        ExecutorService executor = Executors.newFixedThreadPool(numberOfThreads);
        List<Future<?>> futures = new ArrayList<>(taskCount);
        for (int i = 0; i < taskCount; i++) {
            int y = i;
            Future<?> future1 = executor.submit(() -> function.accept(y));
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

    public static void createSSTable(Path dir, LsmConf lsmConf, ArrayList<KVRecord> records) throws IOException {
        records.sort(Comparator.comparing(o -> o.key));
        File dirPath = dir.toFile();
        SSTableWriter ssTableOnDiskCreator = new SSTableWriter(dirPath, lsmConf, records.size());
        ssTableOnDiskCreator.createSSTable(records.iterator());
    }

}
