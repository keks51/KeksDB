package com.keks.kv_storage.server.http;

import com.keks.kv_storage.KVStore;
import com.keks.kv_storage.client.KVServerHttpClient;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import utils.TestUtils;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

import static com.keks.kv_storage.lsm.conf.LsmConfParamsEnum.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;


class KVHttpServerConcurrentTest {

    private static File storageDir;
    private static KVServerHttpClient kvServerHttpClient;
    private static KVHttpServer kvHttpServer;
    private static KVStore kvStore;

    @BeforeAll
    public static void startServer(@TempDir Path dir) throws IOException {
        System.setProperty("jdk.httpclient.allowRestrictedHeaders", "Connection");
        storageDir = dir.toFile();
        int serverPort = 8768;
        String serverHost = "localhost";
        kvServerHttpClient = new KVServerHttpClient(serverHost, serverPort);
        kvStore = new KVStore(storageDir);
        kvHttpServer = new KVHttpServer(serverPort,kvStore, 1, 50);
        kvHttpServer.start();
    }

    //threads,records,index,memory,time
    //20,     50_000, 100,  10_000,
    @Test
    public void testConcurrentWriteAndRead() throws IOException, InterruptedException, ExecutionException, TimeoutException, URISyntaxException {
        int recordsNumber = 2_000;
        int numberOfThreads = 20;
        String dbName = "test_db";
        String tableName = "test";

        Properties properties = new Properties();
        properties.put(SPARSE_INDEX_SIZE_RECORDS, 100);
        properties.put(MEM_CACHE_SIZE_RECORDS, 500);
        properties.put(BLOOM_FILTER_FALSE_POSITIVE_RATE, 0.1);
        kvStore.createDB(dbName);
        kvStore.createTable(dbName, tableName, "lsm", properties);


        Instant start = Instant.now();

        Function<Integer, String> func1 = i -> {
            String key = "key" + i;
            String value = "value" + i;
            try {
                kvServerHttpClient.sendPutEntityRequest(dbName, tableName, key, value);
                assertEquals(value, new String(kvServerHttpClient.sendGetEntityRequest(dbName, tableName, key).body()));
                if (i % 3 == 0) kvServerHttpClient.sendRemoveEntityRequest(dbName, tableName, key);
            } catch (IOException e) {
                e.printStackTrace();
                throw new RuntimeException();
            } catch (URISyntaxException | InterruptedException e) {
                e.printStackTrace();
            }
            return "";
        };
        TestUtils.runConcurrentTest(recordsNumber, 10000, 1, func1, numberOfThreads);

        Function<Integer, String> func2 = i -> {
            String key = "key" + i;
            String value = "value" + i;
            try {
                if (i % 3 == 0) {
                    assertNull(kvServerHttpClient.sendGetEntityRequest(dbName, tableName, key).body());
                } else {
                    assertEquals(value, new String(kvServerHttpClient.sendGetEntityRequest(dbName, tableName, key).body()));
                }
            } catch (IOException e) {
                e.printStackTrace();
                throw new RuntimeException();
            } catch (URISyntaxException | InterruptedException e) {
                e.printStackTrace();
            }
            return "";
        };
        TestUtils.runConcurrentTest(recordsNumber, 10000,1, func2, numberOfThreads);

        kvServerHttpClient.sendFlushTableRequest(dbName, tableName);
        kvServerHttpClient.sendOptimizeTableRequest(dbName, tableName);
        TestUtils.runConcurrentTest(recordsNumber, 10000,1, func2, numberOfThreads);

//        assertEquals(recordsNumber * 2/3, kvStore.getTableStatistics(tableName).getTotalNumberOfRecordsWithDeleted());
//        System.out.println("End");
        Instant finish = Instant.now();
        Duration between = Duration.between(start, finish);
//        System.out.println(between);
    }

    @AfterAll
    public static void stopServer() {
        kvHttpServer.stop();
    }

}