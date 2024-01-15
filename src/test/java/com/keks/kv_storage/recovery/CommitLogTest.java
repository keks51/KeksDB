package com.keks.kv_storage.recovery;

import com.keks.kv_storage.record.KVRecord;
import com.keks.kv_storage.bplus.bitmask.AtomicIntegerRoundRobin;
import com.keks.kv_storage.utils.Time;
import com.keks.kv_storage.utils.UnCheckedConsumer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;


class CommitLogTest {

    @ParameterizedTest
    @ValueSource(ints = {1_000, 5_000, 10_000, 15_000, 20_000, 50_000, 75_000, 100_000})
    public void test1(int records, @TempDir Path dir) throws IOException {

        File commitLogFile = new File(dir.toFile(), RecoveryManager.COMMIT_LOG_DIR_NAME);
        int logFiles = 10;
        {
            CommitLogAppender commitLogAppender = new CommitLogAppender(commitLogFile, logFiles);
            AtomicIntegerRoundRobin roundRobin = new AtomicIntegerRoundRobin(5);
            for (int i = 0; i < records; i++) {
                String key = "key" + i;
                String value;
                if (i % 3 == 0) {
                    value = "";
                } else {
                    value = "value".repeat(roundRobin.next()) + i;
                }

                KVRecord kvRecord = new KVRecord(key, value.getBytes());
                commitLogAppender.append(kvRecord);
            }
            commitLogAppender.close();
        }

        {
            CommitLogReader commitLogReader = new CommitLogReader(commitLogFile, logFiles);
            int i = 0;
            AtomicIntegerRoundRobin roundRobin = new AtomicIntegerRoundRobin(5);
            while (commitLogReader.hasNext()) {
                SeqIdKvRecord next = commitLogReader.next();
                KVRecord kvRecord = next.kvRecord;
                String key = "key" + i;
                String value;
                if (i % 3 == 0) {
                    value = "";
                } else {
                    value = "value".repeat(roundRobin.next()) + i;
                }
                assertEquals(i, next.id );
                assertEquals(key, kvRecord.key);
                assertEquals(value, new String(kvRecord.valueBytes));
                i++;
            }
            commitLogReader.close();
            assertEquals(records, i);
        }
    }

    @Test
    public void test2(@TempDir Path dir) throws IOException {
        File commitLogFile = new File(dir.toFile(), RecoveryManager.COMMIT_LOG_DIR_NAME);
        int logFiles = 100;
        {
            CommitLogAppender commitLogAppender = new CommitLogAppender(commitLogFile, logFiles);
            AtomicIntegerRoundRobin roundRobin = new AtomicIntegerRoundRobin(5);
            for (int i = 0; i < 1000; i++) {
                String key = "key" + i;
                String value;
                if (i % 3 == 0) {
                    value = "";
                } else {
                    value = "value".repeat(roundRobin.next()) + i;
                }

                KVRecord kvRecord = new KVRecord(key, value.getBytes());
                commitLogAppender.append(kvRecord);
            }
            commitLogAppender.close();
        }

        {
            CommitLogReader commitLogReader = new CommitLogReader(commitLogFile, logFiles);
            while (commitLogReader.hasNext()) {
                System.out.println(commitLogReader.next());
            }
        }
    }

    //Adding : PT9.154594S
    @ParameterizedTest
    @ValueSource(ints = {0, 2, 5, 9, 10, 11, 100, 500_000})
    public void test3(int records, @TempDir Path dir) throws Exception {
        File commitLogFile = new File(dir.toFile(), RecoveryManager.COMMIT_LOG_DIR_NAME);
        int logFiles = 10;
        CommitLogAppender commitLogAppender = new CommitLogAppender(commitLogFile, logFiles);

//        int records = 1_000_000;
//        int records = 5;

        AtomicInteger atomicInteger = new AtomicInteger(0);
        UnCheckedConsumer<Integer, IOException> func = i -> {
            String key = "key" + i;
            String value = "value" + i;
            commitLogAppender.append(new KVRecord(key, value.getBytes()));
            int i1 = atomicInteger.incrementAndGet();
            if (i1 % 10_000 == 0) System.out.println(i1);
        };


        Time.withTime("Adding", () ->
                runConcurrentTest(records, 10000, 1, func, 100)
        );

        commitLogAppender.close();

        CommitLogReader commitLogReader = new CommitLogReader(commitLogFile, logFiles);
        int id = 0;
        while (commitLogReader.hasNext()) {
            SeqIdKvRecord next = commitLogReader.next();
            assertEquals(id, next.id);
            id++;
        }
        commitLogReader.close();
        assertEquals(records, id);

    }

    public static <T extends Exception> void runConcurrentTest(int taskCount,
                                                               int threadPoolAwaitTimeoutSec,
                                                               int taskAwaitTimeoutSec,
                                                               UnCheckedConsumer<Integer, T> function,
                                                               int numberOfThreads) throws InterruptedException, ExecutionException, TimeoutException {
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

        assertEquals(taskCount, futures.size());
        for (Future<?> future : futures) {
            future.get(taskAwaitTimeoutSec, TimeUnit.SECONDS);
        }

    }


}