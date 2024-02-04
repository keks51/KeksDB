package examples;

import com.keks.kv_storage.KVStore;
import com.keks.kv_storage.lsm.conf.LsmConfParamsEnum;
import com.keks.kv_storage.conf.TableEngineType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.*;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;


public class LsmSamplesTest {

    @Test
    public void ex1(@TempDir Path dir) {

        try (KVStore kvStore = new KVStore(dir.toFile())) {
            // creating db
            String dbName = "lsm_test_db";
            kvStore.createDB(dbName);

            // creating table
            String tblName = "lsm_table_test";
            // lsm table properties
            Properties properties = new Properties() {{
                put(LsmConfParamsEnum.MEM_CACHE_SIZE_RECORDS, 1_000_000);
                put(LsmConfParamsEnum.BLOOM_FILTER_FALSE_POSITIVE_RATE, 0.1);
            }};
            kvStore.createTable(
                    dbName,
                    tblName,
                    TableEngineType.LSM.toString(),
                    properties);

            // adding records
            kvStore.put(dbName, tblName, "key1", "value1".getBytes());
            kvStore.put(dbName, tblName, "key2", "value2".getBytes());
            kvStore.put(dbName, tblName, "key3", "value3".getBytes());

            // getting values
            System.out.println(new String(kvStore.get(dbName, tblName, "key1"))); // value1
            System.out.println(new String(kvStore.get(dbName, tblName, "key2"))); // value2
            System.out.println(new String(kvStore.get(dbName, tblName, "key3"))); // value3
            System.out.println(kvStore.get(dbName, tblName, "key4")); // null

            // deleting several records
            kvStore.remove(dbName, tblName, "key1");
            kvStore.remove(dbName, tblName, "key2");

            // getting records
            System.out.println(kvStore.get(dbName, tblName, "key1")); // null
            System.out.println(kvStore.get(dbName, tblName, "key2")); // null
            System.out.println(new String(kvStore.get(dbName, tblName, "key3"))); // value3
            System.out.println(kvStore.get(dbName, tblName, "key4")); // null

            // dropping table
            kvStore.dropTable(dbName, tblName);

            // dropping db
            kvStore.dropDB(dbName);
        }
    }

    @Test
    public void ex2(@TempDir Path tmpPath) throws InterruptedException, ExecutionException, TimeoutException {
        int numberOfThreads = 100;
        int numberOfRecords = 200_000;
        String dbName = "lsm_test_db";
        String tblName = "lsm_table_test";

        try (KVStore kvStore = new KVStore(tmpPath.toFile());) {
            kvStore.createDB(dbName);
            Properties properties = new Properties() {{
                put(LsmConfParamsEnum.MEM_CACHE_SIZE_RECORDS, 1_000_000);
                put(LsmConfParamsEnum.BLOOM_FILTER_FALSE_POSITIVE_RATE, 0.1);
            }};
            kvStore.createTable(
                    dbName,
                    tblName,
                    TableEngineType.LSM.toString(),
                    properties);

            Consumer<Integer> func = i -> {
                String key = "key" + i;
                String value = "value" + i;

                // adding record
                kvStore.put(dbName, tblName, key, value.getBytes());

                // getting record
                assertEquals(value, new String(kvStore.get(dbName, tblName, key)));

                // remove each third record
                if (i % 3 == 0) {
                    kvStore.remove(dbName, tblName, key);
                    assertNull(kvStore.get(dbName, tblName, key));
                }

            };
            runConcurrentTest(
                    numberOfRecords,
                    func,
                    numberOfThreads);
        }
    }

    public static void runConcurrentTest(int taskCount,
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
        if (!executor.awaitTermination(10000, TimeUnit.SECONDS)) {
            throw new InterruptedException();
        }

        assertEquals(taskCount, futures.size());
        for (Future<?> future : futures) {
            future.get(1, TimeUnit.SECONDS);
        }

    }



}
