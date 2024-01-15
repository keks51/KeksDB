package perf_test.concurrent;

import com.keks.kv_storage.KVStore;
import com.keks.kv_storage.conf.TableEngineType;
import com.keks.kv_storage.utils.Time;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import utils.TestUtils;

import java.nio.file.Path;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static com.keks.kv_storage.conf.ConfigParams.*;
import static org.junit.jupiter.api.Assertions.assertEquals;


class KVStoreBplusConcurrentTest {

    @Test
    public void testKVStore2(@TempDir Path tmpPath) throws Exception {
        int taskCount = 2_000_000;
        int numberOfThreads = 200;
        String dbName = "test1";
        String tableName = "test1";
        KVStore kvStore = new KVStore(tmpPath.toFile());
        kvStore.createDB(dbName);
        Properties properties = new Properties();
        properties.put(KV_TABLE_COMMIT_LOG_PARALLELISM, 10);
        properties.put(BPLUS_FREE_SPACE_CHECKER_CACHE_MAX, 1022);
        properties.put(BPLUS_FREE_SPACE_CHECKER_CACHE_INIT, 1022);
        kvStore.createTable(dbName, tableName, TableEngineType.BPLUS.name(), properties);
        System.out.println(kvStore.getKvTableParameters(dbName, tableName).getAsJson());
        System.out.println(kvStore.getEngineParameters(dbName, tableName).getAsJson());

        AtomicInteger cnt = new AtomicInteger(0);
        Function<Integer, String> func1 = i -> {
            String key = "key" + i;
            String value = "value" + i;
            kvStore.put(dbName, tableName, key, value.getBytes());
//            assertEquals(value, new String(kvStore.get(dbName, tableName, key)));
//            if (i % 3 == 0) kvStore.remove(dbName, tableName, key);
            int i1 = cnt.incrementAndGet();
            if (i1 % 100_000 == 0) System.out.println(i1);
            return "";
        };
        Time.withTime("run", () ->
                TestUtils.runConcurrentTest(taskCount, 10000, 1, func1, numberOfThreads)
        );

    }

//    @Test
    public void testKVStore1(@TempDir Path tmpPath) throws Exception {
        int records = 2_000_000;
        int numberOfThreads = 200;
        String dbName = "test1";
        String tableName = "test1";

        KVStore kvStore = new KVStore(tmpPath.toFile());
        kvStore.createDB(dbName);
        Properties properties = new Properties();
        properties.put(BPLUS_FREE_SPACE_CHECKER_CACHE_MAX, 1022);
        properties.put(BPLUS_FREE_SPACE_CHECKER_CACHE_INIT, 1022);
        properties.put(BPLUS_TREE_ORDER, 451);
        properties.put(KV_TABLE_COMMIT_LOG_PARALLELISM, 10);
        kvStore.createTable(dbName, tableName, TableEngineType.BPLUS.name(), properties);
        System.out.println(kvStore.getEngineParameters(dbName, tableName).getAsJson().toPrettyString());
        System.out.println(kvStore.getKvTableParameters(dbName, tableName).getAsJson().toPrettyString());

        AtomicInteger atomicInteger = new AtomicInteger(0);
        Function<Integer, String> func1 = i -> {
            String key = "key" + i;
            String value = "value" + i;

            kvStore.put(dbName, tableName, key, value.getBytes());
//            assertEquals(value, new String(kvStore.get(dbName, tableName, key)));
//            if (i % 3 == 0) {
//                kvStore.remove(dbName, tableName, key);
//                assertNull(kvStore.get(dbName, tableName, key));
//            }
            int i1 = atomicInteger.incrementAndGet();
            if (i1 % 10_000 == 0) System.out.println(i1);
            return "";
        };

        Time.withTime("Adding",  () ->
                TestUtils.runConcurrentTest(records, 10000, 1, func1, numberOfThreads)
        );

//        Function<Integer, String> func2 = i -> {
//            String key = "key" + i;
//            String value = "value" + i;
//            if (i % 3 == 0) {
//                assertNull(kvStore.get(dbName, tableName, key));
//            } else {
//                assertEquals(value, new String(kvStore.get(dbName, tableName, key)));
//            }
//
//            return "";
//        };
//
//        Time.withTime(
//                "Getting1",
//                () -> TestUtils.runConcurrentTest(records, 10000, 1, func2, numberOfThreads));
//
//
//
//        Time.withTime(
//                "flushing",
//                () -> kvStore.flushTable(dbName, tableName));
//        Time.withTime(
//                "optimizing",
//                () -> kvStore.optimizeTable(dbName, tableName));
//
////        TestUtils.assertNumberOfSSTables(tmpPath.resolve(tableName).toFile(), 1);
//
//        Time.withTime(
//                "Getting2",
//                () -> TestUtils.runConcurrentTest(records, 10000, 1, func2, numberOfThreads)
//        );
    }


}
