package com.keks.kv_storage.server.thrift;

import com.keks.kv_storage.KVStore;
import com.keks.kv_storage.client.KVThriftClient;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.Properties;
import java.util.function.BiFunction;

import static com.keks.kv_storage.lsm.conf.LsmConfParamsEnum.*;
import static com.keks.kv_storage.server.thrift.TestThriftKVStorageWritePerformance.runConcurrent;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;


class KVThriftServerConcurrentTest {

    private static File storageDir;
    private static KVThriftServer thriftServer;
    private static KVStore kvStore;
    private static int serverPort = 8889;
    private static String serverHost = "localhost";

    @BeforeAll
    public static void startServer(@TempDir Path dir) throws IOException, TTransportException {
        storageDir = dir.toFile();
        int defaultNumberOfKeysInBlockIndex = 100;
        int defaultMaxNumberOfRecordsInMemory = 1_000;
        kvStore = new KVStore(storageDir);
        thriftServer = new KVThriftServer(serverPort, kvStore, 1, 50);
        thriftServer.start();
    }


    @AfterAll
    public static void stopServer() {
        thriftServer.stop();
    }

    //threads,records,index,memory,time
    //20,     50_000, 100,  10_000,7M27S
    @Test
    public void testConcurrentWriteAndRead() throws IOException, InterruptedException {
        int recordsNumber = 2_000;
        int numberOfThreads = 20;
        String dbName = "test_db";
        String tableName = "test";

        Properties properties = new Properties();
        properties.put(SPARSE_INDEX_SIZE, 100);
        properties.put(MEM_CACHE_SIZE, 500);
        properties.put(BLOOM_FILTER_FALSE_POSITIVE_RATE, 0.1);
        kvStore.createDB(dbName);
        kvStore.createTable(dbName, tableName, "lsm", properties);
        Instant start = Instant.now();

        BiFunction<Integer, Integer, String> func1 = (leftBound, rightBound) -> {
            try {
                System.out.println("Running: " + leftBound + " -> " + rightBound);
                KVThriftClient kvThriftClient = new KVThriftClient("localhost", serverPort);
                for (int i = leftBound; i < rightBound; i++) {
                    String key = "key" + i;
                    String value = "value" + i;
                    kvThriftClient.putEntity(dbName, tableName, key, value);
                    assertEquals(value, new String(kvThriftClient.getEntity(dbName, tableName, key)));
                    if (i % 3 == 0) kvThriftClient.removeEntity(dbName, tableName, key);
                }
                kvThriftClient.close();
                System.out.println("END: " + leftBound + " -> " + rightBound);
            } catch (TException e) {
                e.printStackTrace();
                throw new RuntimeException();
            }
            return "";
        };
        runConcurrent(numberOfThreads, recordsNumber, func1);


        BiFunction<Integer, Integer, String> func2 = (leftBound, rightBound) -> {
            try {
                System.out.println("Running: " + leftBound + " -> " + rightBound);
                KVThriftClient kvThriftClient = new KVThriftClient("localhost", serverPort);
                for (int i = leftBound; i < rightBound; i++) {
                    String key = "key" + i;
                    String value = "value" + i;
                    if (i % 3 == 0) {
                        assertNull(kvThriftClient.getEntity(dbName, tableName, key));
                    } else {
                        assertEquals(value, new String(kvThriftClient.getEntity(dbName, tableName, key)));
                    }
                }
                kvThriftClient.close();
                System.out.println("END: " + leftBound + " -> " + rightBound);
            } catch (TException e) {
                e.printStackTrace();
                throw new RuntimeException();
            }
            return "";
        };
        runConcurrent(numberOfThreads, recordsNumber, func2);

        kvStore.flushTable(dbName, tableName);
        kvStore.optimizeTable(dbName, tableName);
        runConcurrent(numberOfThreads, recordsNumber, func2);

        System.out.println("End");
        Instant finish = Instant.now();
        Duration between = Duration.between(start, finish);
        System.out.println(between);
    }


}
