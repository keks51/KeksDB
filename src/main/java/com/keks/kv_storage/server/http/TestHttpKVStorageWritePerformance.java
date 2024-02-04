package com.keks.kv_storage.server.http;

import com.keks.kv_storage.client.KVServerHttpClient;
import com.keks.kv_storage.conf.ConfigParams;
import com.keks.kv_storage.conf.TableEngineType;
import com.keks.kv_storage.utils.UnCheckedConsumer;

import java.io.IOException;
import java.net.URISyntaxException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.*;
import java.util.function.Function;

public class TestHttpKVStorageWritePerformance {
    //
    //0.3_core,PT12M40.7S
    //1.0_core,PT06M04.2S
    //2.0_core,PT03M50.8S
    //100t,4.0_core,PT03M15.2S
    //150t,4.0_core,PT2M44.849192S
    //500t,4.0_core,PT2M31.182533S

//    hostName,serverPort,recordsNumber,numberOfThreads,sparseIndexSize,inMemoryRecords
    //kv-serer 8765 1000000 150 100 1000
    //50t,100_000,23S
    //50t,1_000_000,3M16S

    //threads,records  ,index,memory,time
    //20,     1_000_000, 1000,00_000,5M32S
    public static void run(String hostName,
                           int httpServerPort,
                           int recordsNumber,
                           int numberOfThreads,
                           int sparseIndexSize,
                           int inMemoryRecords,
                           double bloomFilterFalsePositiveRate,
                           String tableName) throws URISyntaxException, IOException, InterruptedException, ExecutionException, TimeoutException {
        System.setProperty("jdk.httpclient.allowRestrictedHeaders", "Connection");

        String dbName = "test_db";

        KVServerHttpClient kvServerHttpClient1 = new KVServerHttpClient(hostName, httpServerPort, 1);
        kvServerHttpClient1.sendCreateDBRequest(dbName);
        kvServerHttpClient1.sendDropTableRequest(dbName, tableName);

        kvServerHttpClient1.sendCreateTableRequest(dbName, tableName, TableEngineType.LSM, new HashMap<>() {{
            put(ConfigParams.LSM_SPARSE_INDEX_SIZE_RECORDS, sparseIndexSize);
            put(ConfigParams.LSM_MEM_CACHE_SIZE, inMemoryRecords);
            put(ConfigParams.LSM_BLOOM_FILTER_FALSE_POSITIVE_RATE, bloomFilterFalsePositiveRate);
        }});
        Instant start = Instant.now();

        Function<Integer, String> func1 = i -> {
            String key = "key" + i;
            String value = "value" + i;
            try {
                if (i == 40) {
                    System.out.println("fdfdf 40");
                }
                kvServerHttpClient1.sendPutEntityRequest(dbName, tableName, key, value);

            } catch (IOException e) {
                e.printStackTrace();
                throw new RuntimeException();
            } catch (URISyntaxException | InterruptedException e) {
                e.printStackTrace();
            }
            return "";
        };
        runConcurrentTest(recordsNumber, 600_000, 1, func1, numberOfThreads);
        Instant finish = Instant.now();
        Duration between = Duration.between(start, finish);
        System.out.println(between);

    }

    public static void runConcurrentTest(int taskCount,
                                         int threadPoolAwaitTimeoutSec,
                                         int taskAwaitTimeoutSec,
                                         Function<Integer, String> function,
                                         int numberOfThreads) throws InterruptedException, ExecutionException, TimeoutException {
        ExecutorService executor = Executors.newFixedThreadPool(numberOfThreads);
        List<Future<?>> futures = new ArrayList<>(taskCount);
        for (int i = 0; i < taskCount; i++) {
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
        }

    }

    public static <EX extends Exception> void runConcurrentTest(int taskCount,
                                                                int threadPoolAwaitTimeoutSec,
                                                                int taskAwaitTimeoutSec,
                                                                UnCheckedConsumer<Integer, EX> function,
                                                                int numberOfThreads) throws InterruptedException, ExecutionException, TimeoutException, EX {
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
        }

    }

}
