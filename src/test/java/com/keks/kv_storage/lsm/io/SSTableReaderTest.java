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
        int rec = 10_0;
        ArrayList<KVRecord> records = new ArrayList<>(rec);
        for (int i = 0; i < rec; i++) {
            records.add(new KVRecord("key" + String.format("%08d", i), ("value" + i).getBytes()));
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
            ArrayList<IndexedKey> sparseIndexes = ssTableReader.readSparseIndex();
            FileChannel indexChannel = FileChannel.open(new File(dir.toFile(), DENSE_INDEX_FILE_NAME).toPath(), StandardOpenOption.READ);
            for (int i = 0; i < sparseIndexes.size(); i++) {
                IndexedKey sparseIndex = sparseIndexes.get(i);
                long blockStartPos = sparseIndex.posInReferenceFile;
                SSTableReader.DenseIndexReader denseIndexReader = new SSTableReader.DenseIndexReader(indexChannel, numberOfKeysInIndex, blockStartPos, 32 * 1024);
                int cnt = 0;
                while (denseIndexReader.hasNext()) {
                    denseIndexReader.next();
                    cnt++;
                }
                if (i == sparseIndexes.size() - 2) { // only works when rec = 10_000;
                    assertEquals(rec - numberOfKeysInIndex * (rec / numberOfKeysInIndex), cnt);
                } else if (i == sparseIndexes.size() - 1) { // only works when rec = 10_000;
                    assertEquals(1, cnt); // maxKey
                } else {
                    assertEquals(numberOfKeysInIndex, cnt);

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
    public void testFindKeyInIndexBlock(@TempDir Path parentDir) throws IOException, ExecutionException, InterruptedException, TimeoutException {

        for (int numberOfKeysInIndex = 1; numberOfKeysInIndex < 20; numberOfKeysInIndex++) {
            for (int rec = 1; rec < 100; rec++) {
                Path dir = parentDir.resolve(numberOfKeysInIndex + "").resolve(rec + "");
                dir.toFile().mkdirs();

                LsmConf lsmConf = new LsmConf(numberOfKeysInIndex, 10, 0.5);
                ArrayList<KVRecord> records = new ArrayList<>(rec);
                for (int i = 0; i < rec; i++) {
                    records.add(new KVRecord("key" + String.format("%05d", i), ("value" + i).getBytes()));
                }
                createSSTable(dir, lsmConf, records);

                SSTableReader ssTableReader = new SSTableReader(dir.toFile(), numberOfKeysInIndex);
                ArrayList<IndexedKey> sparseIndexedKeys = ssTableReader.readSparseIndex();

                for (int sparseIdx = 0; sparseIdx < sparseIndexedKeys.size(); sparseIdx++) {
                    IndexedKey sparseIndex = sparseIndexedKeys.get(sparseIdx);
                    long indexBlockStartPos = sparseIndex.posInReferenceFile;
                    for (int keyId = 0; keyId < rec; keyId++) {
                        String expKey = "key" + String.format("%05d", keyId);
                        IndexedKey keyInIndexBlock = ssTableReader.findKeyInDenseIndex(expKey, indexBlockStartPos, sparseIndex.refRecordLen);
                        int leftBound = sparseIdx * numberOfKeysInIndex;
                        int rightBound = leftBound + numberOfKeysInIndex;

                        // last block
                        if (sparseIdx == sparseIndexedKeys.size() - 1) {
                            if (keyId == rec - 1) {
                                assertEquals(expKey, keyInIndexBlock.key);
                            }else {
                                assertNull(keyInIndexBlock);
                            }
                        } else {
                            // regular block
                            if (leftBound <= keyId && keyId < rightBound ) { // right condition is for maxKey
                                assertEquals(expKey, keyInIndexBlock.key);
                            } else {
                                assertNull(keyInIndexBlock);
                            }
                        }

                    }
                }
            }

        }
    }

    @Test
    public void testFindKeyInIndexBlockConcurrent(@TempDir Path dir) throws IOException, ExecutionException, InterruptedException, TimeoutException {
        int numberOfKeysInIndex = 13;
        LsmConf lsmConf = new LsmConf(numberOfKeysInIndex, 10, 0.5);
        int rec = 1_000;
        ArrayList<KVRecord> records = new ArrayList<>(rec);
        for (int i = 0; i < rec; i++) {
            records.add(new KVRecord("key" + String.format("%05d", i), ("value" + i).getBytes()));
        }
        createSSTable(dir, lsmConf, records);

        SSTableReader ssTableReader = new SSTableReader(dir.toFile(), numberOfKeysInIndex);
        ArrayList<IndexedKey> sparseIndexedKeys = ssTableReader.readSparseIndex();

        Consumer<Integer> func = sparseIdx -> {
            try {
                IndexedKey sparseIndex = sparseIndexedKeys.get(sparseIdx);
                long indexBlockStartPos = sparseIndex.posInReferenceFile;
                for (int keyId = 0; keyId < rec; keyId++) {
                    String expKey = "key" + String.format("%05d", keyId);
                    IndexedKey keyInIndexBlock = ssTableReader.findKeyInDenseIndex(expKey, indexBlockStartPos, sparseIndex.refRecordLen);
                    int leftBound = sparseIdx * numberOfKeysInIndex;
                    int rightBound = leftBound + numberOfKeysInIndex;

                    // last block
                    if (sparseIdx == sparseIndexedKeys.size() - 1) {
                        if (keyId == rec - 1) {
                            assertEquals(expKey, keyInIndexBlock.key);
                        }else {
                            assertNull(keyInIndexBlock);
                        }
                    } else {
                        // regular block
                        if (leftBound <= keyId && keyId < rightBound ) { // right condition is for maxKey
                            assertEquals(expKey, keyInIndexBlock.key);
                        } else {
                            assertNull(keyInIndexBlock);
                        }
                    }

                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        };
        runConcurrentTest(sparseIndexedKeys.size() - 1, 1000, 10, func, 1);
    }

    @Test
    public void testFindKeyInDenseIndexBlock0(@TempDir Path parentDir) throws IOException, ExecutionException, InterruptedException, TimeoutException {
        for (int numberOfKeysInIndex = 1; numberOfKeysInIndex < 20; numberOfKeysInIndex++) {
            for (int rec = 1; rec < 100; rec++) {
                Path dir = parentDir.resolve(numberOfKeysInIndex + "").resolve(rec + "");
                dir.toFile().mkdirs();
                LsmConf lsmConf = new LsmConf(numberOfKeysInIndex, 10, 0.5);
                ArrayList<KVRecord> records = new ArrayList<>(rec);
                for (int i = 0; i < rec; i++) {
                    records.add(new KVRecord("key" + String.format("%05d", i), ("value" + i).getBytes()));
                }
                createDenseIndexSSTable(dir, lsmConf, records);

                SSTableReader ssTableReader = new SSTableReader(dir.toFile(), numberOfKeysInIndex);
                ArrayList<IndexedKey> sparseIndexedKeys = ssTableReader.readSparseIndex();

                for (int sparseIdx = 0; sparseIdx < sparseIndexedKeys.size() - 1; sparseIdx++) {
                    IndexedKey sparseIndex = sparseIndexedKeys.get(sparseIdx);
                    long indexBlockStartPos = sparseIndex.posInReferenceFile;
                    int indexBlockLen = sparseIndex.refRecordLen;
                    String leftKey = sparseIndex.key;
                    String rightKey = sparseIndexedKeys.get(sparseIdx + 1).key;

                    for (int keyNum = 0; keyNum < rec; keyNum++) {
                        String expKey = "key" + String.format("%05d", keyNum);
                        IndexedKey keyInIndexBlock = ssTableReader.findKeyInDenseBlockIndex(expKey, indexBlockStartPos, indexBlockLen);
                        if (expKey.compareTo(leftKey) >= 0 && (expKey.compareTo(rightKey) < 0)
                                || leftKey.equals(rightKey)
                                || keyNum == rec - 1 && sparseIndexedKeys.get(sparseIdx + 1).posInReferenceFile == indexBlockStartPos
                        ) {
                            assertEquals(expKey, keyInIndexBlock.key);
                        } else {
                            assertNull(keyInIndexBlock);
                        }
                    }

                }

                if (numberOfKeysInIndex != 1 && rec % numberOfKeysInIndex != 1) { // last block should contain more than one key
                    IndexedKey maxKey = sparseIndexedKeys.get(sparseIndexedKeys.size() - 1);
                    IndexedKey preMaxKey = sparseIndexedKeys.get(sparseIndexedKeys.size() - 2);
                    IndexedKey keyInDenseIndex = ssTableReader.findKeyInDenseBlockIndex(maxKey.key, preMaxKey.posInReferenceFile, preMaxKey.refRecordLen);
                    assertEquals(maxKey.key, keyInDenseIndex.key);
                }
            }
        }
    }


    @Test
    public void testFindKeyInDenseIndexBlockConcurrent(@TempDir Path dir) throws IOException, ExecutionException, InterruptedException, TimeoutException {
        int numberOfKeysInIndex = 13;
        LsmConf lsmConf = new LsmConf(numberOfKeysInIndex, 10, 0.5);
        int rec = 1_000;
        ArrayList<KVRecord> records = new ArrayList<>(rec);
        for (int i = 0; i < rec; i++) {
            records.add(new KVRecord("key" + String.format("%05d", i), ("value" + i).getBytes()));
        }
        createDenseIndexSSTable(dir, lsmConf, records);

        SSTableReader ssTableReader = new SSTableReader(dir.toFile(), numberOfKeysInIndex);
        ArrayList<IndexedKey> sparseIndexedKeys = ssTableReader.readSparseIndex();

        Consumer<Integer> func = sparseIdx -> {
            try {
                IndexedKey sparseIndex = sparseIndexedKeys.get(sparseIdx);
                long indexBlockStartPos = sparseIndex.posInReferenceFile;
                int indexBlockLen = sparseIndex.refRecordLen;
                String leftKey = sparseIndex.key;
                String rightKey = sparseIndexedKeys.get(sparseIdx + 1).key;

                for (int keyNum = 0; keyNum < rec; keyNum++) {
                    String expKey = "key" + String.format("%05d", keyNum);
                    IndexedKey keyInIndexBlock = ssTableReader.findKeyInDenseBlockIndex(expKey, indexBlockStartPos, indexBlockLen);
                    if (expKey.compareTo(leftKey) >= 0 && (expKey.compareTo(rightKey) < 0)
                            || leftKey.equals(rightKey)
                            || keyNum == rec - 1 && sparseIndexedKeys.get(sparseIdx + 1).posInReferenceFile == indexBlockStartPos
                    ) {
                        assertEquals(expKey, keyInIndexBlock.key);
                    } else {
                        assertNull(keyInIndexBlock);
                    }
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        };
        runConcurrentTest(sparseIndexedKeys.size() - 1, 1000, 10, func, 1);
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

    public static void createDenseIndexSSTable(Path dir, LsmConf lsmConf, ArrayList<KVRecord> records) throws IOException {
        records.sort(Comparator.comparing(o -> o.key));
        File dirPath = dir.toFile();
        SSTableWriterWithDenseBlock ssTableOnDiskCreator = new SSTableWriterWithDenseBlock(dirPath, lsmConf, records.size());
        ssTableOnDiskCreator.createSSTable(records.iterator());
    }

}
