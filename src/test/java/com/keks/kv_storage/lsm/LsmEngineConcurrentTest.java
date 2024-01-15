package com.keks.kv_storage.lsm;

import com.keks.kv_storage.record.KVRecord;
import com.keks.kv_storage.lsm.conf.LsmConf;
import com.keks.kv_storage.query.Query;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.io.TempDir;
import utils.TestScheduler;
import utils.TestUtils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;


class LsmEngineConcurrentTest implements TestScheduler {

    @RepeatedTest(10)
    public void testConcurrentWriteAndRead1(@TempDir Path tmpPath) throws IOException, ExecutionException, InterruptedException, TimeoutException {
        LsmConf lsmConf = new LsmConf(10, 50, 0.90, false, 10, false, 0, 10);
        Path tableDir = tmpPath.resolve("test_table");
        Files.createDirectory(tableDir);
        testConcurrentWriteAndRead(tableDir.toFile(), lsmConf);
    }

    @RepeatedTest(10)
    public void testConcurrentWriteAndRead2(@TempDir Path tmpPath) throws IOException, ExecutionException, InterruptedException, TimeoutException {
        LsmConf lsmConf = new LsmConf(10, 50, 0.90, true, 10, false, 0, 10);
        Path tableDir = tmpPath.resolve("test_table");
        Files.createDirectory(tableDir);
        testConcurrentWriteAndRead(tableDir.toFile(), lsmConf);
    }

    @RepeatedTest(10)
    public void testConcurrentWriteAndRead3(@TempDir Path tmpPath) throws IOException, ExecutionException, InterruptedException, TimeoutException {
        LsmConf lsmConf = new LsmConf(10, 50, 0.90, false, 10, true, 0, 10);
        Path tableDir = tmpPath.resolve("test_table");
        Files.createDirectory(tableDir);
        testConcurrentWriteAndRead(tableDir.toFile(), lsmConf);
    }

    @RepeatedTest(10)
    public void testConcurrentWriteAndRead4(@TempDir Path tmpPath) throws IOException, ExecutionException, InterruptedException, TimeoutException {
        LsmConf lsmConf = new LsmConf(10, 50, 0.90, true, 10, true, 0, 10);
        Path tableDir = tmpPath.resolve("test_table");
        Files.createDirectory(tableDir);
        testConcurrentWriteAndRead(tableDir.toFile(), lsmConf);
    }

    // same as testConcurrentWriteAndRead4 but with different memCacheSize
    @RepeatedTest(10)
    public void testConcurrentWriteAndRead5(@TempDir Path tmpPath) throws IOException, ExecutionException, InterruptedException, TimeoutException {
        LsmConf lsmConf = new LsmConf(2, 5, 0.90, true, 10, true, 0, 10);
        Path tableDir = tmpPath.resolve("test_table");
        Files.createDirectory(tableDir);
        testConcurrentWriteAndRead(tableDir.toFile(), lsmConf);
    }

    public void testConcurrentWriteAndRead(File tableDir, LsmConf lsmConf) throws IOException, InterruptedException, ExecutionException, TimeoutException {
        System.out.println("Dir: " + tableDir);
        int taskCount = 10_000;
        int numberOfThreads = 50;


        LsmEngine lsmEngine = LsmEngine.createNewTable("", tableDir, lsmConf, scheduler);

        Instant start = Instant.now();
        AtomicInteger recordsCnt = new AtomicInteger();

        Set<Integer> completed = Collections.newSetFromMap(new ConcurrentHashMap<>());
        Function<Integer, String> func1 = i -> {
            try {
                String key = "key" + String.format("%05d", i);
                String value = "value" + i;
                recordsCnt.incrementAndGet();
                lsmEngine.put(key, value);

                assertEquals(value, new String(lsmEngine.get(key).valueBytes));
                if (i % 3 == 0) {
                    lsmEngine.remove(key);
                    recordsCnt.decrementAndGet();
                    assertNull(lsmEngine.get(key));
                } else {
                    assertEquals(value, new String(lsmEngine.get(key).valueBytes));
                }
                if (i % 100 == 0) {
                    Integer[] array = completed.toArray(new Integer[0]);
                    ArrayList<KVRecord> asArr = lsmEngine.getRangeRecords(Query.QUERY_ALL).getAsArr();
                    Set<String> kvRecordsSet = asArr.stream().map(e -> e.key).collect(Collectors.toSet());
                    for (Integer integer : array) {
                        String completedKey = "key" + String.format("%05d", integer);
                        if (integer % 3 == 0) {
                            if (kvRecordsSet.contains(completedKey)) {
                                System.out.println();
                            }
                            assertFalse(kvRecordsSet.contains(completedKey));
                        } else {
                            assertTrue(kvRecordsSet.contains(completedKey));
                        }
                    }
                }

                completed.add(i);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            return "";
        };
        TestUtils.runConcurrentTest(taskCount, 10000, 10, func1, numberOfThreads);


        System.out.println("here0");
        lsmEngine.forceFlush();
        System.out.println("here1");

        Function<Integer, String> func2 = i -> {
            try {
                String key = "key" + String.format("%05d", i);
                String value = "value" + i;
                if (i % 3 == 0) {
                    assertNull(lsmEngine.get(key));
                } else {
                    KVRecord kvRecord = lsmEngine.get(key);
                    if (kvRecord == null) {
                        System.out.println();
                    }
                    assertEquals(value, new String(lsmEngine.get(key).valueBytes));
                }
                if (i % 100 == 0) {
                    Integer[] array = completed.toArray(new Integer[0]);
                    ArrayList<KVRecord> asArr = lsmEngine.getRangeRecords(Query.QUERY_ALL).getAsArr();
                    Set<String> kvRecordsSet = asArr.stream().map(e -> e.key).collect(Collectors.toSet());
                    for (Integer integer : array) {
                        String completedKey = "key" + String.format("%05d", integer);
                        if (integer % 3 == 0) {
                            if (kvRecordsSet.contains(completedKey)) {
                                System.out.println();
                            }
                            assertFalse(kvRecordsSet.contains(completedKey));
                        } else {
                            assertTrue(kvRecordsSet.contains(completedKey));
                        }
                    }
                }
            } catch (Throwable e) {
                throw new RuntimeException(e);
            }
            return "";
        };
        TestUtils.runConcurrentTest(taskCount, 10000, 10, func2, numberOfThreads);

        System.out.println("here2");
        lsmEngine.forceFlush();
        System.out.println("here3");

        lsmEngine.optimize();
        System.out.println("here4");


        TestUtils.assertNumberOfSSTables(tableDir, 1);

        TestUtils.runConcurrentTest(taskCount, 10000, 10, func2, numberOfThreads);

        Instant finish = Instant.now();
        Duration between = Duration.between(start, finish);
        System.out.println(between);
        lsmEngine.close();

    }

}
