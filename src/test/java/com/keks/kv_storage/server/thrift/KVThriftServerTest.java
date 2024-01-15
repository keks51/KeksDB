package com.keks.kv_storage.server.thrift;

import com.keks.kv_storage.KVStore;
import com.keks.kv_storage.client.KVThriftClient;
import com.keks.kv_storage.conf.ConfigParams;
import com.keks.kv_storage.conf.TableEngineType;
import com.keks.kv_storage.ex.*;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import utils.TestUtils;

import java.io.File;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

import static com.keks.kv_storage.lsm.conf.LsmConfParamsEnum.MEM_CACHE_SIZE;
import static com.keks.kv_storage.lsm.conf.LsmConfParamsEnum.SPARSE_INDEX_SIZE;
import static org.junit.jupiter.api.Assertions.*;


class KVThriftServerTest {

    private static File storageDir;
    private static Path storageDirPath;
    private static KVThriftServer thriftServer;
    private static KVStore kvStore;
    private static int serverPort = 8889;
    KVThriftClient kvThriftClient;

    static String dbName = "test_db";

    @BeforeAll
    public static void startServer(@TempDir Path dir) throws TTransportException {
        storageDir = dir.toFile();
        storageDirPath = dir;
        int defaultNumberOfKeysInBlockIndex = 100;
        int defaultMaxNumberOfRecordsInMemory = 1_000;
        kvStore = new KVStore(storageDir);
        thriftServer = new KVThriftServer(serverPort, kvStore, 1, 5);
        thriftServer.start();
        kvStore.createDB(dbName);
    }

    @BeforeEach
    public void createClient() throws TTransportException {
        kvThriftClient = new KVThriftClient("localhost", serverPort);
    }

    @Test
    public void testCreateDB() throws TException {
        String dbName = "test_db_create";

        {
            kvThriftClient.createDB(dbName);
            assertDbExists(dbName);
        }

        {
            ThriftClientCommandException exception = assertThrows(ThriftClientCommandException.class,
                    () -> kvThriftClient.createDB(dbName));
            assertEquals(DatabaseAlreadyExistsException.class.getName(), exception.className);
        }

    }

    @Test
    public void testDropDB() throws TException {
        String dbName = "test_db_drop";
        kvStore.createDB(dbName);
        assertDbExists(dbName);

        {
            kvThriftClient.dropDB(dbName);
            assertDbNotExists(dbName);
        }

        {
            ThriftClientCommandException exception = assertThrows(ThriftClientCommandException.class,
                    () -> kvThriftClient.dropDB(dbName));
            assertEquals(DatabaseNotFoundException.class.getName(), exception.className);
        }
    }

    @Test
    public void testCreateTable() throws TException {
        String tableName = "test_create_table";
        {
            kvThriftClient.createTable(dbName, tableName, TableEngineType.LSM, new HashMap<>() {
                {
                    put(ConfigParams.LSM_SPARSE_INDEX_SIZE, 3);
                    put(ConfigParams.LSM_MEM_CACHE_SIZE, 10);
                    put(ConfigParams.LSM_BLOOM_FILTER_FALSE_POSITIVE_RATE, 0.1);
                }
            });
            assertTableExists(dbName, tableName);
        }

        {
            ThriftClientCommandException exception = assertThrows(ThriftClientCommandException.class,
                    () -> kvThriftClient.createTable(dbName, tableName, TableEngineType.LSM, new HashMap<>() {
                        {
                            put(ConfigParams.LSM_SPARSE_INDEX_SIZE, 3);
                            put(ConfigParams.LSM_MEM_CACHE_SIZE, 10);
                            put(ConfigParams.LSM_BLOOM_FILTER_FALSE_POSITIVE_RATE, 0.1);
                        }
                    }));
            assertEquals(TableAlreadyExistsException.class.getName(), exception.className);
        }
    }

    @Test
    public void testDropDB2() {
        String dbName = "test_db_drop2";
        String tableName = "table1";
        kvStore.createDB(dbName);
        kvStore.createTable(dbName, tableName, TableEngineType.LSM.toString(), new Properties());

        {
            ThriftClientCommandException exception = assertThrows(ThriftClientCommandException.class,
                    () -> kvThriftClient.dropDB(dbName));
            assertEquals(DatabaseNotEmptyException.class.getName(), exception.className);
        }
    }

    @Test
    public void testDropTable() throws TException {
        String tableName = "test_drop_table";

        kvStore.createTable(dbName, tableName, TableEngineType.LSM.toString(), new Properties());
        {
            kvThriftClient.dropTable(dbName, tableName);
            assertTableNotExists(dbName, tableName);
        }

        {
            ThriftClientCommandException exception = assertThrows(ThriftClientCommandException.class,
                    () -> kvThriftClient.dropTable(dbName, tableName));
            assertEquals(TableNotFoundException.class.getName(), exception.className);
        }
    }

    @Test
    public void testDropTable2() throws TException {
        String tableName = "test_drop_table2";
        Properties properties = new Properties() {{
            put(SPARSE_INDEX_SIZE, 1);
            put(MEM_CACHE_SIZE, 1);
        }};
        kvStore.createTable(dbName, tableName, TableEngineType.LSM.toString(), properties);
        kvStore.put(dbName, tableName,"key12", "age12".getBytes());

        kvThriftClient.dropTable(dbName, tableName);
        assertTableNotExists(dbName, tableName);
    }

    @Test
    public void testPutEntity() throws TException {
        String tableName = "test_put_entity";
        kvStore.createTable(dbName, tableName, TableEngineType.LSM.toString(), new Properties());
        String key = "key12";
        String value = "age12";
        kvThriftClient.putEntity(dbName, tableName, key, value);

        String actValue = new String(kvStore.get(dbName, tableName, key));
        assertEquals(value, actValue);
    }

    @Test
    public void testPutEntity2() throws TException {
        String tableName = "test_put_entity2";
        kvStore.createTable(dbName, tableName, TableEngineType.LSM.toString(), new Properties());
        String key = "key12";
        String value = "age12";
        kvThriftClient.putEntity(dbName, tableName, key, value.getBytes());

        String actValue = new String(kvStore.get(dbName, tableName, key));
        assertEquals(value, actValue);
    }

    @Test
    public void testRemoveEntity() throws TException {
        String tableName = "test_remove_entity";
        kvStore.createTable(dbName, tableName, TableEngineType.LSM.toString(), new Properties());
        String key = "key12";
        String value = "age12";
        kvStore.put(dbName, tableName, key, value.getBytes());

        kvThriftClient.removeEntity(dbName, tableName, key);

        assertNull(kvStore.get(dbName, tableName, key));
    }

    @Test
    public void testGetEntity() throws TException {
        String tableName = "test_get_entity";
        kvStore.createTable(dbName, tableName, TableEngineType.LSM.toString(), new Properties());
        String key = "key12";
        String value = "age12";
        kvStore.put(dbName, tableName, key, value.getBytes());

        String entityStr = new String(kvThriftClient.getEntity(dbName, tableName, key));

        assertEquals(entityStr, value);
    }

    @Test
    public void testGetEntity2() throws TException {
        String tableName = "test_get_entity2";
        kvStore.createTable(dbName, tableName, TableEngineType.LSM.toString(), new Properties());
        String key = "key12";

        assertNull(kvThriftClient.getEntity(dbName, tableName, key));
    }

    @Test
    public void testOptimizeTable() throws TException {
        String tableName = "test_optimize";
        Properties properties = new Properties() {{
            put(SPARSE_INDEX_SIZE, 1);
            put(MEM_CACHE_SIZE, 1);
        }};
        kvStore.createTable(dbName, tableName, TableEngineType.LSM.toString(), properties);

        kvStore.put(dbName, tableName, "key12", "age12".getBytes());
        kvStore.put(dbName, tableName, "key13", "age14".getBytes());
        kvStore.put(dbName, tableName, "key14", "age14".getBytes());
        kvStore.flushTable(dbName, tableName);

        File tableDir = storageDirPath.resolve(dbName).resolve(tableName).resolve(TableEngineType.LSM.toString()).resolve("data").toFile();
        TestUtils.assertNumberOfSSTables(tableDir, 3);

        kvThriftClient.optimizeTable(dbName, tableName);
        TestUtils.assertNumberOfSSTables(tableDir, 1);
    }

    @Test
    public void testFlushTable() throws TException {
        String tableName = "test_flush";
        Properties properties = new Properties() {{
            put(SPARSE_INDEX_SIZE, 1);
            put(MEM_CACHE_SIZE, 2);
        }};
        kvStore.createTable(dbName, tableName, TableEngineType.LSM.toString(), properties);


        kvStore.put(dbName, tableName, "key12", "age12".getBytes());
        kvStore.put(dbName, tableName, "key13", "age14".getBytes());
        kvStore.put(dbName, tableName, "key14", "age14".getBytes());
        File tableDir = storageDirPath.resolve(dbName).resolve(tableName).resolve(TableEngineType.LSM.toString()).resolve("data").toFile();
        TestUtils.assertNumberOfSSTables(tableDir, 1);

        kvThriftClient.flushTable(dbName, tableName);
        TestUtils.assertNumberOfSSTables(tableDir, 2);
    }

    @Test
    public void testCreateCheckpoint() throws TException {
        String tableName = "test_checkpoint";
        Path tableDir = storageDirPath.resolve(dbName).resolve(tableName);
        File dataDir = tableDir.resolve(TableEngineType.LSM.toString()).resolve("data").toFile();
        {
            Properties properties = new Properties() {{
                put(SPARSE_INDEX_SIZE, 1);
                put(MEM_CACHE_SIZE, 2);
            }};
            kvStore.createTable(dbName, tableName, TableEngineType.LSM.toString(), properties);
        }


        {
            kvStore.put(dbName, tableName, "key12", "age12".getBytes());
            kvStore.put(dbName, tableName, "key13", "age14".getBytes());
            kvStore.put(dbName, tableName, "key14", "age15".getBytes());
            TestUtils.assertNumberOfSSTables(dataDir, 1);
            kvThriftClient.createCheckpoint(dbName, tableName);
            TestUtils.assertNumberOfSSTables(dataDir, 2);
            TestUtils.assertCheckpointDirContainsLsmSSTables(tableDir.toFile(), "1v1", "2v1");
        }

        {
            kvStore.put(dbName, tableName, "key22", "age12".getBytes());
            kvStore.put(dbName, tableName, "key23", "age14".getBytes());
            kvStore.put(dbName, tableName, "key24", "age15".getBytes());
            TestUtils.assertSStablesExists(dataDir, "1v1", "2v1", "3v1");
            kvThriftClient.createCheckpoint(dbName, tableName);
            TestUtils.assertSStablesExists(dataDir, "1v1", "2v1", "3v1", "4v1");
            TestUtils.assertCheckpointDirContainsLsmSSTables(tableDir.toFile(), "1v1", "2v1", "3v1", "4v1");
        }
    }

    @Test
    public void testGetDBs() throws TException {
        String dbName1 = "test_get_db1";
        String dbName2 = "test_get_db2";
        kvStore.createDB(dbName1);
        kvStore.createDB(dbName2);

        List<String> databases = kvThriftClient.getDatabases();


        HashSet<String> dbs = new HashSet<>(databases);
        assertTrue(dbs.contains(dbName1.toUpperCase()));
        assertTrue(dbs.contains(dbName2.toUpperCase()));
    }

    @Test
    public void testGetTables() throws TException {

        String table1 = "test_get_tables1";
        String table2 = "test_get_tables2";
        kvStore.createTable(dbName, table1, TableEngineType.LSM.toString(), new Properties());
        kvStore.createTable(dbName, table2, TableEngineType.LSM.toString(), new Properties());

        List<String> tables = kvThriftClient.getTables(dbName);


        HashSet<String> dbs = new HashSet<>(tables);
        assertTrue(dbs.contains(table1.toUpperCase()));
        assertTrue(dbs.contains(table2.toUpperCase()));
    }

    @AfterEach
    public void closeClient() throws TTransportException {
        kvThriftClient.close();
    }

    @AfterAll
    public static void stopServer() {
        thriftServer.stop();
    }

    private void assertDbExists(String dbName) {
        List<String> databasesList = kvStore.getDatabasesList().stream().map(String::toUpperCase).collect(Collectors.toList());;
        assertTrue(databasesList.contains(dbName.toUpperCase()));
        File dbDir = new File(kvStore.kvStoreDir, dbName);
        assertTrue(dbDir.exists());
    }

    private void assertDbNotExists(String dbName) {
        List<String> databasesList = kvStore.getDatabasesList().stream().map(String::toUpperCase).collect(Collectors.toList());;
        assertFalse(databasesList.contains(dbName.toUpperCase()));
        File dbDir = new File(kvStore.kvStoreDir, dbName);
        assertFalse(dbDir.exists());
    }

    private void assertTableExists(String dbName, String tableName) {
        List<String> tablesList = kvStore.getTablesList(dbName).stream().map(String::toUpperCase).collect(Collectors.toList());
        assertTrue(tablesList.contains(tableName.toUpperCase()));
        File dbDir = new File(kvStore.kvStoreDir, dbName);
        File tableDir = new File(dbDir, tableName);
        assertTrue(tableDir.exists());
    }

    private void assertTableNotExists(String dbName, String tableName) {
        List<String> tablesList = kvStore.getTablesList(dbName).stream().map(String::toUpperCase).collect(Collectors.toList());
        assertFalse(tablesList.contains(tableName.toUpperCase()));
        File dbDir = new File(kvStore.kvStoreDir, dbName);
        File tableDir = new File(dbDir, tableName);
        assertFalse(tableDir.exists());
    }

}
