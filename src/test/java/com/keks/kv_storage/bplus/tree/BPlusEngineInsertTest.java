package com.keks.kv_storage.bplus.tree;


import com.keks.kv_storage.bplus.BPlusEngine;
import com.keks.kv_storage.bplus.buffer.PageBuffer;
import com.keks.kv_storage.bplus.TableName;
import com.keks.kv_storage.bplus.conf.BPlusConf;
import com.keks.kv_storage.bplus.conf.BtreeConf;
import com.keks.kv_storage.record.KVRecord;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertLinesMatch;


// TODO check freeSpace statistics to understand how many pages are free after deletion
public class BPlusEngineInsertTest {

    private static final TableName tableName = new TableName("test");

    @Test
    public void test1(@TempDir Path dir) throws IOException {
        BPlusConf btreeParams = new BPlusConf(3, 10, 10,400_000);
        BPlusEngine kvTable = BPlusEngine.createNewTable("", dir.toFile(), btreeParams);

        String key8 = "key008";
        String value8 = "value" + "key008";
        kvTable.addWithWriteLock(new KVRecord("key008", ("value" + "key008").getBytes()));
        String key12 = "key012";
        String value12 = "value" + "key012";
        kvTable.addWithWriteLock(new KVRecord("key012", ("value" + "key012").getBytes()));
        String key6 = "key006";
        String value6 = "value" + "key006";
        kvTable.addWithWriteLock(new KVRecord("key006", ("value" + "key006").getBytes()));
        String key5 = "key005";
        String value5 = "value" + "key005";
        kvTable.addWithWriteLock(new KVRecord("key005", ("value" + "key005").getBytes()));
        String key10 = "key010";
        String value10 = "value" + "key010";
        kvTable.addWithWriteLock(new KVRecord("key010", ("value" + "key010").getBytes()));
        String key9 = "key009";
        String value9 = "value" + "key009";
        kvTable.addWithWriteLock(new KVRecord("key009", ("value" + "key009").getBytes()));
        String key7 = "key007";
        String value7 = "value" + "key007";
        kvTable.addWithWriteLock(new KVRecord("key007", ("value" + "key007").getBytes()));
        String key4 = "key004";
        String value4 = "value" + "key004";
        kvTable.addWithWriteLock(new KVRecord("key004", ("value" + "key004").getBytes()));


        String key2 = "key002";
        String value2 = "value" + "key002";
        kvTable.addWithWriteLock(new KVRecord("key002", ("value" + "key002").getBytes()));


        String key1 = "key001";
        String value1 = "value" + "key001";
        kvTable.addWithWriteLock(new KVRecord("key001", ("value" + "key001").getBytes()));
        String key11 = "key011";
        String value11 = "value" + "key011";
        kvTable.addWithWriteLock(new KVRecord("key011", ("value" + "key011").getBytes()));
        String key3 = "key003";
        String value3 = "value" + "key003";
        kvTable.addWithWriteLock(new KVRecord("key003", ("value" + "key003").getBytes()));

        assertEquals(value1, new String(kvTable.get(key1).valueBytes));
        assertEquals(value2, new String(kvTable.get(key2).valueBytes));
        assertEquals(value3, new String(kvTable.get(key3).valueBytes));
        assertEquals(value4, new String(kvTable.get(key4).valueBytes));
        assertEquals(value5, new String(kvTable.get(key5).valueBytes));
        assertEquals(value6, new String(kvTable.get(key6).valueBytes));
        assertEquals(value7, new String(kvTable.get(key7).valueBytes));
        assertEquals(value8, new String(kvTable.get(key8).valueBytes));
        assertEquals(value9, new String(kvTable.get(key9).valueBytes));
        assertEquals(value10, new String(kvTable.get(key10).valueBytes));
        assertEquals(value11, new String(kvTable.get(key11).valueBytes));
        assertEquals(value12, new String(kvTable.get(key12).valueBytes));

        List<String> expAll = new ArrayList<>() {
            {
                add(value1);
                add(value2);
                add(value3);
                add(value4);
                add(value5);
                add(value6);
                add(value7);
                add(value8);
                add(value9);
                add(value10);
                add(value11);
                add(value12);
            }
        };

        List<String> actAll = kvTable.getAll().stream().map(e -> new String(e.valueBytes)).collect(Collectors.toList());

        assertLinesMatch(expAll, actAll);

    }

    @ParameterizedTest
    @ValueSource(ints = {3, 5, 10, 31, 100, 201})
    public void test2(int order, @TempDir Path dir) throws IOException {
        BPlusConf btreeParams = new BPlusConf(order, 10, 10,400_000);
        BPlusEngine kvTable = BPlusEngine.createNewTable("", dir.toFile(), btreeParams);
        int maxValue = 10 * order;
        HashMap<String, String> kvMap = new HashMap<>();
        ArrayList<String> keys = new ArrayList<>();
        Collections.shuffle(keys);

        {
            for (int i = 0; i <= maxValue; i++) {
                String key = "key" + String.format("%03d", i);
                keys.add(key);
            }

            for (int i = 0; i <= maxValue; i++) {
                String key = keys.get(i);
                String value = "value" + key;

                kvTable.addWithWriteLock(new KVRecord(key, value.getBytes()));

                assertEquals(value, new String(kvTable.get(key).valueBytes));

                if (i % 2 == 0) {
                    value = "value_new" + key;
                    kvTable.addWithWriteLock(new KVRecord(key, value.getBytes()));
                    assertEquals(value, new String(kvTable.get(key).valueBytes));

                }
                kvMap.put(key, value);

            }
            for (int i = 0; i <= maxValue; i++) {
                String key = keys.get(i);
                String value = "value" + key;
                if (i % 2 == 0) {
                    value = "value_new" + key;
                }
                assertEquals(value, new String(kvTable.get(key).valueBytes));
            }

            ArrayList<KVRecord> all = kvTable.getAll();
            for (KVRecord keyDataTuple : all) {
                assertEquals(new KVRecord(keyDataTuple.key, kvMap.get(keyDataTuple.key).getBytes()), keyDataTuple);
            }


        }
//        myBuffer.flushAll();
        {
            for (int i = 0; i <= maxValue; i++) {
                String key = keys.get(i);
                String value = "value" + key;
                if (i % 2 == 0) {
                    value = "value_new" + key;
                }
                assertEquals(value, new String(kvTable.get(key).valueBytes));
            }

            ArrayList<KVRecord> all = kvTable.getAll();
            for (KVRecord keyDataTuple : all) {
                assertEquals(new KVRecord(keyDataTuple.key, kvMap.get(keyDataTuple.key).getBytes()), keyDataTuple);
            }
        }

    }

    @RepeatedTest(10)
    public void test3(@TempDir Path dir) throws IOException {
        BPlusConf btreeParams = new BPlusConf(3, 10, 10,400_000);
        BPlusEngine kvTable = BPlusEngine.createNewTable("", dir.toFile(), btreeParams);

        ArrayList<Integer> keys = new ArrayList<>();
        for (int i = 0; i < 500; i++) {
            keys.add(i);
        }

        Collections.shuffle(keys);

        for (Integer i : keys) {
            String key = "key" + String.format("%05d", i);
            String value = "value" + i;
            System.out.println(key);
            kvTable.addWithWriteLock(new KVRecord(key, value.getBytes()));

            assertEquals(value, new String(kvTable.get(key).valueBytes));
        }

    }

    // insert and update
    @RepeatedTest(10)
    public void test4(@TempDir Path dir) throws IOException, ExecutionException, InterruptedException, TimeoutException {

        BPlusConf btreeParams = new BPlusConf(BtreeConf.MAX_ORDER, 10, 10,400_000_000);
        BPlusEngine kvTable = BPlusEngine.createNewTable("", dir.toFile(), btreeParams);


        System.out.println("Running");
        Function<Integer, String> func = i -> {
            String key = "";
            try {
                key = "key" + String.format("%05d", i);
                String value = "value" + i;

                kvTable.addWithWriteLock(new KVRecord(key, value.getBytes()));

                assertEquals(value, new String(kvTable.get(key).valueBytes));

                if (i % 2 == 0) {
                    value = "value_new" + i;
                    kvTable.addWithWriteLock(new KVRecord(key, value.getBytes()));
                    assertEquals(value, new String(kvTable.get(key).valueBytes));
                }

                return "";
            } catch (IOException e) {
                e.printStackTrace();
                throw new RuntimeException();
            } catch (Throwable t) {
                System.out.println("Failed on key: " + key);
                try {
                    kvTable.printTree();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                t.printStackTrace();

                System.exit(1);
                throw t;
            }

        };

        runConcurrentTest(50_000, 1000, 1, func, 400);
        kvTable.close();
    }



    public static void runConcurrentTest(int taskCount,
                                         int threadPoolAwaitTimeoutSec,
                                         int taskAwaitTimeoutSec,
                                         Function<Integer, String> function,
                                         int numberOfThreads) throws InterruptedException, ExecutionException, TimeoutException {
        ExecutorService executor = Executors.newFixedThreadPool(numberOfThreads);
        List<Future<?>> futures = new ArrayList<>(taskCount);
        for (int i = 1; i <= taskCount; i++) {
            int y = i;
            Future<?> future1 = executor.submit(() -> function.apply(y));
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
