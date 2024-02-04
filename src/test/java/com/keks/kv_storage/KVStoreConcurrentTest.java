package com.keks.kv_storage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import utils.TestUtils;

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


class KVStoreConcurrentTest {

    @Test
    public void testConcurrentWriteAndRead(@TempDir Path tmpPath) throws InterruptedException, ExecutionException, TimeoutException {
        int taskCount = 200_000;
        String dbName = "db_test";
        String tableName = "test1";
        KVStore kvStore = new KVStore(tmpPath.toFile()); //, 100, 500, 0.1
        kvStore.createDB(dbName);
        Properties properties = new Properties();
        properties.put(SPARSE_INDEX_SIZE_RECORDS, 100);
        properties.put(MEM_CACHE_SIZE_RECORDS, 100_000);
        properties.put(BLOOM_FILTER_FALSE_POSITIVE_RATE, 0.9);
        kvStore.createTable(dbName, tableName, "lsm", properties);

        Function<Integer, String> func1 = i -> {
            String key = "key" + i;
            String value = "value" + i;
            kvStore.put(dbName, tableName, key, value.getBytes());
            assertEquals(value, new String(kvStore.get(dbName, tableName, key)));
            if (i % 3 == 0) kvStore.remove(dbName, tableName, key);
            return "";
        };
        TestUtils.runConcurrentTest(taskCount, 10000, 1, func1, 200);

        Function<Integer, String> func2 = i -> {
            String key = "key" + i;
            String value = "value" + i;
            if (i % 3 == 0) {
                assertNull(kvStore.get(dbName, tableName, key));
            } else {
                assertEquals(value, new String(kvStore.get(dbName, tableName, key)));
            }
            return "";
        };
        TestUtils.runConcurrentTest(taskCount, 10000, 1, func2, 32);

        kvStore.flushTable(dbName, tableName);
        kvStore.optimizeTable(dbName, tableName);
        TestUtils.assertNumberOfSSTables(tmpPath.resolve(dbName).resolve(tableName).toFile(), 1);



        Instant start = Instant.now();
        TestUtils.runConcurrentTest(taskCount, 10000, 1, func2, 32);
        Instant finish = Instant.now();
        Duration between = Duration.between(start, finish);
//        System.out.println(between);

    }


}
