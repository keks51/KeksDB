package com.keks.kv_storage;

import com.keks.kv_storage.bplus.conf.BPlusConfParamsEnum;
import com.keks.kv_storage.client.KVServerHttpClient;
import com.keks.kv_storage.conf.TableEngineType;
import com.keks.kv_storage.ex.*;
import com.keks.kv_storage.lsm.conf.LsmConfParamsEnum;
import com.keks.kv_storage.server.http.KVHttpServer;
import com.keks.kv_storage.utils.UnCheckedBiConsumer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import utils.TestUtils;

import java.io.IOException;
import java.net.ConnectException;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.*;


class KVStoreTest {

    // KvStore is empty. All methods should return exception
    @Test
    public void test1(@TempDir Path dir) {
        KVStore kvStore = new KVStore(dir.toFile());

        Assertions.assertThrows(
                DatabaseNotFoundException.class,
                () -> kvStore.dropDB("test_db1"));

        Assertions.assertThrows(
                DatabaseNotFoundException.class,
                () -> kvStore.createTable("test_db1", "test1", TableEngineType.LSM.toString(), new Properties()));

        Assertions.assertThrows(
                DatabaseNotFoundException.class,
                () -> kvStore.dropTable("test_db1", "test1"));

        Assertions.assertThrows(
                DatabaseNotFoundException.class,
                () -> kvStore.put("test_db1", "test1", "key1", "data".getBytes()));

        Assertions.assertThrows(
                DatabaseNotFoundException.class,
                () -> kvStore.get("test_db1", "test1", "key1"));

        Assertions.assertThrows(
                DatabaseNotFoundException.class,
                () -> kvStore.remove("test_db1", "test1", "key1"));

        assertTrue(kvStore.getDatabasesList().isEmpty());
    }

    // creating and deleting one DB
    @Test
    public void test2(@TempDir Path dir) {
        KVStore kvStore = new KVStore(dir.toFile());

        String dbName = "test_db";
        assertTrue(kvStore.createDB(dbName));

        assertEquals(new ArrayList<>(List.of(dbName.toUpperCase())), kvStore.getDatabasesList());

        Assertions.assertThrows(
                DatabaseAlreadyExistsException.class,
                () -> kvStore.createDB(dbName));
        Assertions.assertThrows(
                DatabaseAlreadyExistsException.class,
                () -> kvStore.createDB(dbName.toUpperCase()));

        Assertions.assertThrows(
                TableNotFoundException.class,
                () -> kvStore.dropTable(dbName, "test1"));

        Assertions.assertThrows(
                TableNotFoundException.class,
                () -> kvStore.put(dbName, "test1", "key1", "data".getBytes()));

        Assertions.assertThrows(
                TableNotFoundException.class,
                () -> kvStore.get(dbName, "test1", "key1"));

        Assertions.assertThrows(
                TableNotFoundException.class,
                () -> kvStore.remove(dbName, "test1", "key1"));

        kvStore.dropDB(dbName);

        assertTrue(kvStore.getDatabasesList().isEmpty());
    }

    // creating and updating one table
    @Test
    public void test3Lsm(@TempDir Path dir) {
        KVStore kvStore = new KVStore(dir.toFile());

        String dbName = "test_db";
        String tableName = "test1";
        assertTrue(kvStore.createDB(dbName));
        kvStore.createTable(dbName, tableName, "lsm", new Properties());
        assertEquals(new ArrayList<>(List.of(tableName.toUpperCase())), kvStore.getTablesList(dbName));

        HashMap<String, Object> conf = kvStore.getEngineParameters(dbName, tableName).getAsMap();
        assertEquals(LsmConfParamsEnum.SPARSE_INDEX_SIZE.defaultValue, conf.get(LsmConfParamsEnum.SPARSE_INDEX_SIZE.name));
        assertEquals(LsmConfParamsEnum.MEM_CACHE_SIZE.defaultValue, conf.get(LsmConfParamsEnum.MEM_CACHE_SIZE.name));
        assertEquals(LsmConfParamsEnum.BLOOM_FILTER_FALSE_POSITIVE_RATE.defaultValue, conf.get(LsmConfParamsEnum.BLOOM_FILTER_FALSE_POSITIVE_RATE.name));

        Assertions.assertThrows(
                TableAlreadyExistsException.class,
                () -> kvStore.createTable(dbName, tableName, "lsm", new Properties()));

        Assertions.assertThrows(
                TableAlreadyExistsException.class,
                () -> kvStore.createTable(dbName, tableName.toUpperCase(), "lsm", new Properties()));

        Assertions.assertThrows(
                TableAlreadyExistsException.class,
                () -> kvStore.createTable(dbName.toUpperCase(), tableName, "lsm", new Properties()));

        Assertions.assertThrows(
                TableAlreadyExistsException.class,
                () -> kvStore.createTable(dbName.toUpperCase(), tableName.toUpperCase(), "lsm", new Properties()));

        kvStore.put(dbName, tableName, "key1", "haha1".getBytes());
        kvStore.put(dbName, tableName, "key2", "haha2".getBytes());
        assertEquals("haha1", new String(kvStore.get(dbName, tableName, "key1")));
        assertEquals("haha2", new String(kvStore.get(dbName, tableName, "key2")));

        kvStore.remove(dbName, tableName, "key1");
        kvStore.remove(dbName, tableName, "key2");
        assertNull(kvStore.get(dbName, tableName, "key1"));
        assertNull(kvStore.get(dbName, tableName, "key2"));

        kvStore.put(dbName, tableName, "key1", "haha1".getBytes());
        kvStore.put(dbName, tableName, "key2", "haha2".getBytes());
        assertEquals("haha1", new String(kvStore.get(dbName, tableName, "key1")));
        assertEquals("haha2", new String(kvStore.get(dbName, tableName, "key2")));

        kvStore.dropTable(dbName, tableName);

        assertFalse(dir.resolve(dbName).resolve(tableName).toFile().exists());
    }

    // creating and updating one table
    @Test
    public void test3BPlus(@TempDir Path dir) {
        KVStore kvStore = new KVStore(dir.toFile());

        String dbName = "test_db";
        String tableName = "test1";
        assertTrue(kvStore.createDB(dbName));
        kvStore.createTable(dbName, tableName, "bplus", new Properties());
        assertEquals(new ArrayList<>(List.of(tableName.toUpperCase())), kvStore.getTablesList(dbName));

        HashMap<String, Object> conf = kvStore.getEngineParameters(dbName, tableName).getAsMap();
        assertEquals(BPlusConfParamsEnum.BTREE_ORDER.defaultValue, conf.get(BPlusConfParamsEnum.BTREE_ORDER.name));

        Assertions.assertThrows(
                TableAlreadyExistsException.class,
                () -> kvStore.createTable(dbName, tableName, "bplus", new Properties()));

        Assertions.assertThrows(
                TableAlreadyExistsException.class,
                () -> kvStore.createTable(dbName, tableName.toUpperCase(), "bplus", new Properties()));

        Assertions.assertThrows(
                TableAlreadyExistsException.class,
                () -> kvStore.createTable(dbName.toUpperCase(), tableName, "bplus", new Properties()));

        Assertions.assertThrows(
                TableAlreadyExistsException.class,
                () -> kvStore.createTable(dbName.toUpperCase(), tableName.toUpperCase(), "bplus", new Properties()));

        kvStore.put(dbName, tableName, "key1", "haha1".getBytes());
        kvStore.put(dbName, tableName, "key2", "haha2".getBytes());
        assertEquals("haha1", new String(kvStore.get(dbName, tableName, "key1")));
        assertEquals("haha2", new String(kvStore.get(dbName, tableName, "key2")));

        kvStore.remove(dbName, tableName, "key1");
        kvStore.remove(dbName, tableName, "key2");
        assertNull(kvStore.get(dbName, tableName, "key1"));
        assertNull(kvStore.get(dbName, tableName, "key2"));

        kvStore.put(dbName, tableName, "key1", "haha1".getBytes());
        kvStore.put(dbName, tableName, "key2", "haha2".getBytes());
        assertEquals("haha1", new String(kvStore.get(dbName, tableName, "key1")));
        assertEquals("haha2", new String(kvStore.get(dbName, tableName, "key2")));

        kvStore.dropTable(dbName, tableName);

        assertFalse(dir.resolve(dbName).resolve(tableName).toFile().exists());
    }

    // creating and deleting table
    @Test
    public void test4Lsm(@TempDir Path dir) {
        KVStore kvStore = new KVStore(dir.toFile());

        String dbName = "test_db";
        String tableName = "test1";
        assertTrue(kvStore.createDB(dbName));
        kvStore.createTable(dbName, tableName, "lsm", new Properties());
        kvStore.put(dbName, tableName, "key1", "haha1".getBytes());
        kvStore.put(dbName, tableName, "key2", "haha2".getBytes());
        assertEquals("haha1", new String(kvStore.get(dbName, tableName, "key1")));
        assertEquals("haha2", new String(kvStore.get(dbName, tableName, "key2")));

        kvStore.dropTable(dbName, tableName);
        assertFalse(dir.resolve(dbName).resolve(tableName).toFile().exists());

        kvStore.dropDB(dbName);
        assertFalse(dir.resolve(dbName).toFile().exists());
    }

    // creating and deleting table
    @Test
    public void test4BPlus(@TempDir Path dir) {
        KVStore kvStore = new KVStore(dir.toFile());

        String dbName = "test_db";
        String tableName = "test1";
        assertTrue(kvStore.createDB(dbName));
        kvStore.createTable(dbName, tableName, "bplus", new Properties());
        kvStore.put(dbName, tableName, "key1", "haha1".getBytes());
        kvStore.put(dbName, tableName, "key2", "haha2".getBytes());
        assertEquals("haha1", new String(kvStore.get(dbName, tableName, "key1")));
        assertEquals("haha2", new String(kvStore.get(dbName, tableName, "key2")));

        kvStore.dropTable(dbName, tableName);
        assertFalse(dir.resolve(dbName).resolve(tableName).toFile().exists());

        kvStore.dropDB(dbName);
        assertFalse(dir.resolve(dbName).toFile().exists());
    }

    // creating table and deleting database
    @Test
    public void test5(@TempDir Path dir) {
        KVStore kvStore = new KVStore(dir.toFile());

        String dbName = "test_db";
        String tableName = "test1";
        assertTrue(kvStore.createDB(dbName));
        kvStore.createTable(dbName, tableName, "lsm", new Properties());
        kvStore.put(dbName, tableName, "key1", "haha1".getBytes());
        kvStore.put(dbName, tableName, "key2", "haha2".getBytes());
        assertEquals("haha1", new String(kvStore.get(dbName, tableName, "key1")));
        assertEquals("haha2", new String(kvStore.get(dbName, tableName, "key2")));

        Assertions.assertThrows(
                DatabaseNotEmptyException.class,
                () -> kvStore.dropDB(dbName));
    }


    // create and drop dbs concurrently
    @Test
    public void test6(@TempDir Path dir) throws ExecutionException, InterruptedException, TimeoutException {
        KVStore kvStore = new KVStore(dir.toFile());
        int dbCnt = 20_000;
        int tablesCnt = 20;

        AtomicInteger cnt = new AtomicInteger();
        Consumer<Integer> func = i -> {
            String dbName = "db" + i;
            kvStore.createDB(dbName);
            assertTrue(kvStore.getDatabasesList().contains(dbName.toUpperCase()));
            kvStore.dropDB(dbName);
            assertFalse(kvStore.getDatabasesList().contains(dbName.toUpperCase()));
            int i1 = cnt.incrementAndGet();
            if (i1 % 10_000 == 0) System.out.println(i1);
        };

        runConcurrentTest(dbCnt, 1000, 1, func, 2000);

        TestUtils.assertDirIsEmpty(dir);
    }

    // create and drop dbs and tables concurrently
    @Test
    public void test7(@TempDir Path dir) throws ExecutionException, InterruptedException, TimeoutException {
        KVStore kvStore = new KVStore(dir.toFile());
        int dbCnt = 10;
        int tablesCnt = 50;


        for (int dbId = 0; dbId < dbCnt; dbId++) {
            String dbName = "db" + dbId;
            kvStore.createDB(dbName);
        }
        ArrayList<String> databasesList = kvStore.getDatabasesList();

        AtomicInteger cnt = new AtomicInteger();
        AtomicInteger tableIdCnt = new AtomicInteger();
        Consumer<Integer> func = dbId -> {
            for (int i = 0; i < tablesCnt; i++) {
                String dbName = databasesList.get(new Random().nextInt(dbCnt));
                int tableId = tableIdCnt.getAndIncrement();
                String tableName = "table" + tableId;
                String tableEngine;
                Properties properties = new Properties();
                if (i % 2 == 0) {
                    tableEngine = "lsm";
                } else {
                    tableEngine = "bplus";
                    properties.put(BPlusConfParamsEnum.FREE_SPACE_CHECKER_CACHE_MAX, 1);
                    properties.put(BPlusConfParamsEnum.FREE_SPACE_CHECKER_CACHE_INIT, 1);
                }

                kvStore.createTable(dbName, tableName, tableEngine, properties);

                assertTrue(kvStore.getTablesList(dbName).contains(tableName.toUpperCase()));

                kvStore.dropTable(dbName, tableName);

                assertFalse(kvStore.getTablesList(dbName).contains(tableName.toUpperCase()));
                int i1 = cnt.incrementAndGet();
//                if (i1 % 10_000 == 0)
                System.out.println(i1);
            }
//            kvStore2.dropDB(dbName);
//            assertFalse(kvStore2.getDatabasesList().contains(dbName.toUpperCase()));


        };

        runConcurrentTest(dbCnt, 1000, 1, func, 10);

    }

    @Test
    public void testCreateReadLsm(@TempDir Path dir) {

        String dbName1 = "test_db1";
        String dbName2 = "test_db2";
        String tableName1 = "test1";
        String tableName2 = "test2";
        String tableName3 = "test3";
        String tableName4 = "test4";
        String keyPostfix1 = dbName1 + tableName1;
        String keyPostfix2 = dbName1 + tableName2;
        String keyPostfix3 = dbName2 + tableName3;
        String keyPostfix4 = dbName2 + tableName4;

        {
            KVStore kvStore = new KVStore(dir.toFile());
            {
                kvStore.createDB(dbName1);
                kvStore.createTable(dbName1, tableName1, TableEngineType.LSM.name(), new Properties());
                kvStore.put(dbName1, tableName1, "key1" + keyPostfix1, "haha1".getBytes());
                kvStore.put(dbName1, tableName1, "key2" + keyPostfix1, "haha2".getBytes());
            }
            {
                kvStore.createTable(dbName1, tableName2, TableEngineType.LSM.name(), new Properties());
                kvStore.put(dbName1, tableName2, "key1" + keyPostfix2, "haha1".getBytes());
                kvStore.put(dbName1, tableName2, "key2" + keyPostfix2, "haha2".getBytes());
            }
            {
                kvStore.createDB(dbName2);
                kvStore.createTable(dbName2, tableName3, TableEngineType.LSM.name(), new Properties());
                kvStore.put(dbName2, tableName3, "key1" + keyPostfix3, "haha1".getBytes());
                kvStore.put(dbName2, tableName3, "key2" + keyPostfix3, "haha2".getBytes());
            }
            {
                kvStore.createTable(dbName2, tableName4, TableEngineType.LSM.name(), new Properties());
                kvStore.put(dbName2, tableName4, "key1" + keyPostfix4, "haha1".getBytes());
                kvStore.put(dbName2, tableName4, "key2" + keyPostfix4, "haha2".getBytes());
            }
            kvStore.close();
        }

        {
            KVStore kvStore = new KVStore(dir.toFile());
            {
                assertEquals("haha1", new String(kvStore.get(dbName1, tableName1, "key1" + keyPostfix1)));
                assertEquals("haha2", new String(kvStore.get(dbName1, tableName1, "key2" + keyPostfix1)));
            }
            {
                assertEquals("haha1", new String(kvStore.get(dbName1, tableName2, "key1" + keyPostfix2)));
                assertEquals("haha2", new String(kvStore.get(dbName1, tableName2, "key2" + keyPostfix2)));
            }
            {
                assertEquals("haha1", new String(kvStore.get(dbName2, tableName3, "key1" + keyPostfix3)));
                assertEquals("haha2", new String(kvStore.get(dbName2, tableName3, "key2" + keyPostfix3)));
            }
            {
                assertEquals("haha1", new String(kvStore.get(dbName2, tableName4, "key1" + keyPostfix4)));
                assertEquals("haha2", new String(kvStore.get(dbName2, tableName4, "key2" + keyPostfix4)));
            }
            kvStore.close();
        }
    }

    @Test
    public void testKvStoreCloseLsm(@TempDir Path dir) throws ExecutionException, InterruptedException, TimeoutException, IOException {

        ConcurrentHashMap<String, String> map = new ConcurrentHashMap<>();
        {
            int threads = 200;
            int taskCount = 100_000;
            int dbCnt = 5;
            int tablesCnt = 5;
            String dbName = "db_test";
            String tableName = "test";
            KVStore kvStore = new KVStore(dir.toFile());
            int httpPort = 8088;
            KVHttpServer kvHttpServer = new KVHttpServer(httpPort, kvStore, 1, threads);
            kvHttpServer.start();
            {
                for (int dbId = 0; dbId < dbCnt; dbId++) {
                    String dbNameId = dbName + dbId;
                    kvStore.createDB(dbNameId);
                    for (int tableId = 0; tableId < tablesCnt; tableId++) {
                        TableEngineType tableEngine = (tableId % 2 == 0) ? TableEngineType.LSM : TableEngineType.BPLUS;
                        kvStore.createTable(dbNameId, tableName + tableId, tableEngine.name(), new Properties());
                    }
                }
            }


            AtomicInteger cycles = new AtomicInteger(0);
            UnCheckedBiConsumer<Integer, KVServerHttpClient, Exception> func1 = (i, client) -> {
                int i1 = cycles.incrementAndGet();
                if (i1 % 10_000 == 0) System.out.println(i1);
                if ((taskCount / 2) == i1) {
                    kvStore.close();
                }
                if (i1 < (taskCount / 2)) {
                    int dbId = new Random().nextInt(dbCnt);
                    int tableId = new Random().nextInt(tablesCnt);
                    String dbNameId = dbName + dbId;
                    String tableNameId = tableName + tableId;
                    String key = "key" + i;
                    String value = "value" + i;
                    try {

                        {
                            HttpResponse<String> response = client.sendPutEntityRequest(dbNameId, tableNameId, key, value);
                            if (response.statusCode() == 200) map.put(dbNameId + "&" + tableNameId + "&" + key, value);
                        }

                        if (i % 3 == 0) {
                            HttpResponse<String> response = client.sendRemoveEntityRequest(dbNameId, tableNameId, key);
                            if (response.statusCode() == 200) {
                                map.put(dbNameId + "&" + tableNameId + "&" + key, "");
                            }
                        }

                    } catch (ConnectException ignored) {

                    } catch (Throwable e) {
                        e.printStackTrace();
                        System.exit(1);
                    }
                }


            };

            runConcurrentTest2(taskCount, httpPort, 10000, 1, func1, threads);
        }

        {
            KVStore kvStore = new KVStore(dir.toFile());
            for (Map.Entry<String, String> entry : map.entrySet()) {
                String[] split = entry.getKey().split("&");
                String dbName = split[0];
                String tableName = split[1];
                String key = split[2];
                String value = entry.getValue();
                byte[] actValue = kvStore.get(dbName, tableName, key);
//                if (value.isEmpty()) {
//                    try {
//                        assertNull(actValue);
//                    } catch (AssertionFailedError e) {
//                        System.exit(1);
//                    }
//
//                } else {
//                    try {
//                        assertEquals(value, new String(actValue));
//                    } catch (NullPointerException e) {
//                        System.exit(1);
//                    }
//                }
                if (value.isEmpty()) {
                    assertNull(actValue);
                } else {
                    assertEquals(value, new String(actValue));
                }

            }
        }
    }

    public static <Ex extends Exception> void runConcurrentTest2(int taskCount,
                                                                 int httpPort,
                                                                 int threadPoolAwaitTimeoutSec,
                                                                 int taskAwaitTimeoutSec,
                                                                 UnCheckedBiConsumer<Integer, KVServerHttpClient, Ex> function,
                                                                 int numberOfThreads) throws InterruptedException, ExecutionException, TimeoutException {
        ExecutorService executor = Executors.newFixedThreadPool(numberOfThreads);

        KVServerHttpClient[] httpClients = new KVServerHttpClient[numberOfThreads];
        for (int i = 0; i < numberOfThreads; i++) {
            httpClients[i] = new KVServerHttpClient("localhost", httpPort);
        }

        List<Future<?>> futures = new ArrayList<>(taskCount);
        for (int i = 0; i < taskCount; i++) {
            KVServerHttpClient httpClient = httpClients[i % numberOfThreads];
            int y = i;
            Future<?> future1 = executor.submit(() -> {
                try {
                    function.accept(y, httpClient);
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
//            future.get();
        }

    }

    public static void runConcurrentTest(int taskCount,
                                         int threadPoolAwaitTimeoutSec,
                                         int taskAwaitTimeoutSec,
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
        if (!executor.awaitTermination(threadPoolAwaitTimeoutSec, TimeUnit.SECONDS)) {
            throw new InterruptedException();
        }

        for (Future<?> future : futures) {
            future.get(taskAwaitTimeoutSec, TimeUnit.SECONDS);
//            future.get();
        }

    }

}