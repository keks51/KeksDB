package com.keks.kv_storage.server.thrift;

import com.keks.kv_storage.client.KVThriftClient;
import com.keks.kv_storage.conf.TableEngineType;
import com.keks.kv_storage.server.ThreadUtils;
import org.apache.thrift.TException;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;

public class TestThriftKVStorageWritePerformance {

    //1t,100_000  ,16.6S
    //1t,1_000_000,2M52.6S

    //threads,records  ,index,memory,time
    //20,     1_000_000, 1000,00_000,17S
    public static void run(String dbName,
                           String hostName,
                           TableEngineType engineType,
                           int thriftServerPort,
                           int recordsNumber,
                           int numberOfThreads,
                           int sparseIndexSize,
                           int inMemoryRecords,
                           double bloomFilterFalsePositiveRate,
                           String tableName) throws TException, InterruptedException {

        KVThriftClient kvThriftClientInit = new KVThriftClient(hostName, thriftServerPort);
        try {
//            kvThriftClientInit.deleteKVTable(tableName);
        } catch (Throwable ignored) {}
//        kvThriftClientInit.createTable(dbName, tableName, engineType, sparseIndexSize, inMemoryRecords, bloomFilterFalsePositiveRate);
//        kvThriftClientInit.close();

        AtomicInteger cnt = new AtomicInteger();
        Instant start = Instant.now();
        BiFunction<Integer, Integer, String> func1 = (leftBound, rightBound) -> {
            try {
                System.out.println("Running-" + cnt.addAndGet(1) + ": " + leftBound + " -> " + rightBound);
                KVThriftClient kvThriftClient = new KVThriftClient(hostName, thriftServerPort);
                for (int i = leftBound; i < rightBound; i++) {
                    String key = "key" + i;
                    String value = "value" + i;
//                    kvThriftClient.putEntity(tableName, key, value);
                }
//                kvThriftClient.close();
                System.out.println("END: " + leftBound + " -> " + rightBound);
            } catch (TException e) {
                throw new RuntimeException();
            }
            return "";
        };
        runConcurrent(numberOfThreads, recordsNumber, func1);
        Instant finish = Instant.now();
        Duration between = Duration.between(start, finish);
        System.out.println(between);

    }

    public static void runConcurrent(int numberOfThreads, int recordsNumber, BiFunction<Integer, Integer, String> func) throws InterruptedException {
        ExecutorService pool = ThreadUtils.createCachedThreadPool("KVHttpServer", true);
        int step = recordsNumber / numberOfThreads;
        int startRange = 0;
        for (int i = 0; i < numberOfThreads; i++) {
            int leftBound = startRange;
            int rightBound;
            if (i == numberOfThreads - 1) {
                rightBound = recordsNumber;
            } else {
                rightBound = startRange + step;
            }

            pool.submit(() -> func.apply(leftBound, rightBound));

            startRange = startRange + step;
        }

        pool.shutdown();
        if (!pool.awaitTermination(600, TimeUnit.SECONDS)) {
            throw new InterruptedException();
        }
    }

}
