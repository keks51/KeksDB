package com.keks.kv_storage.server.http;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.keks.kv_storage.record.KVRecord;
import com.keks.kv_storage.KVStore;
import com.keks.kv_storage.client.KVServerHttpClient;
import com.keks.kv_storage.conf.ConfigParams;
import com.keks.kv_storage.conf.TableEngineType;
import com.keks.kv_storage.ex.*;
import com.keks.kv_storage.query.Query;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import utils.TestUtils;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.util.*;

import static com.keks.kv_storage.lsm.conf.LsmConfParamsEnum.*;
import static org.junit.jupiter.api.Assertions.*;


public class KVHttpServerTest {

    private static File storageDir;
    private static Path storageDirPath;
    private static KVServerHttpClient kvServerHttpClient;
    private static KVHttpServer kvHttpServer;
    private static KVStore kvStore;

    static String dbName = "test_db";

    @BeforeAll
    public static void startServer(@TempDir Path dir) throws IOException {
        storageDir = dir.toFile();
        storageDirPath = dir;
        int defaultNumberOfKeysInBlockIndex = 100;
        int defaultMaxNumberOfRecordsInMemory = 1_000;
        int serverPort = 8866;
        String serverHost = "localhost";
        kvServerHttpClient = new KVServerHttpClient(serverHost, serverPort);
        kvStore = new KVStore(storageDir);
        kvHttpServer = new KVHttpServer(serverPort, kvStore, 1, 2);
        kvHttpServer.start();
        kvStore.createDB(dbName);
    }

    @Test
    public void testCreateDB() throws IOException, InterruptedException, URISyntaxException {
        String dbName = "test_db_create";

        {
            HttpResponse<String> httpResponse = kvServerHttpClient.sendCreateDBRequest(dbName);
            assertEquals(200, httpResponse.statusCode());
            assertDbExists(dbName);
        }

        {
            HttpResponse<String> httpResponse = kvServerHttpClient.sendCreateDBRequest(dbName);
            assertEquals(500, httpResponse.statusCode());
            assertException(httpResponse, DatabaseAlreadyExistsException.class);
        }

    }

    @Test
    public void testDropDB() throws IOException, InterruptedException, URISyntaxException {
        String dbName = "test_db_drop";
        kvStore.createDB(dbName);
        assertDbExists(dbName);

        {
            HttpResponse<String> httpResponse = kvServerHttpClient.sendDropDBRequest(dbName);

            assertEquals(200, httpResponse.statusCode());
            assertDbNotExists(dbName);
        }

        {
            HttpResponse<String> httpResponse = kvServerHttpClient.sendDropDBRequest(dbName);

            assertEquals(500, httpResponse.statusCode());
            assertException(httpResponse, DatabaseNotFoundException.class);
        }
    }

    @Test
    public void testCreateTable() throws IOException, InterruptedException, URISyntaxException {
        String tableName = "test_create_table";
        {
            HttpResponse<String> httpResponse = kvServerHttpClient
                    .sendCreateTableRequest(dbName, tableName, TableEngineType.LSM, new HashMap<>() {
                        {
                            put(ConfigParams.LSM_SPARSE_INDEX_SIZE_RECORDS, 3);
                            put(ConfigParams.LSM_MEM_CACHE_SIZE, 10);
                            put(ConfigParams.LSM_BLOOM_FILTER_FALSE_POSITIVE_RATE, 0.1);
                        }
                    });
            System.out.println(httpResponse);
            System.out.println(httpResponse.body());
            assertEquals(200, httpResponse.statusCode());
        }

        {
            HttpResponse<String> httpResponse = kvServerHttpClient
                    .sendCreateTableRequest(dbName, tableName, TableEngineType.LSM, new HashMap<>() {
                        {
                            put(ConfigParams.LSM_SPARSE_INDEX_SIZE_RECORDS, 3);
                            put(ConfigParams.LSM_MEM_CACHE_SIZE, 10);
                            put(ConfigParams.LSM_BLOOM_FILTER_FALSE_POSITIVE_RATE, 0.1);
                        }
                    });
            System.out.println(httpResponse);
            System.out.println(httpResponse.body());
            assertEquals(500, httpResponse.statusCode());
            assertException(httpResponse, TableAlreadyExistsException.class);
        }
    }

    @Test
    public void testDropDB2() throws IOException, InterruptedException, URISyntaxException {
        String dbName = "test_db_drop2";
        String tableName = "table1";
        kvStore.createDB(dbName);
        kvStore.createTable(dbName, tableName, TableEngineType.LSM.toString(), new Properties());


        {
            HttpResponse<String> httpResponse = kvServerHttpClient.sendDropDBRequest(dbName);
            System.out.println(httpResponse.body());
            assertEquals(500, httpResponse.statusCode());
            assertException(httpResponse, DatabaseNotEmptyException.class);
        }
    }

    @Test
    public void testDropTable() throws IOException, InterruptedException, URISyntaxException {
        String tableName = "test_drop_table";

        kvStore.createTable(dbName, tableName, TableEngineType.LSM.toString(), new Properties());
        {
            HttpResponse<String> httpResponse = kvServerHttpClient
                    .sendDropTableRequest(dbName, tableName);
            System.out.println(httpResponse);
            System.out.println(httpResponse.body());
            assertEquals(200, httpResponse.statusCode());
        }

        {
            HttpResponse<String> httpResponse = kvServerHttpClient
                    .sendDropTableRequest(dbName, tableName);
            assertEquals(500, httpResponse.statusCode());
            assertException(httpResponse, TableNotFoundException.class);
        }
    }

    @Test
    public void testDropTable2() throws IOException, InterruptedException, URISyntaxException {
        String tableName = "test_drop_table2";
        Properties properties = new Properties() {{
            put(SPARSE_INDEX_SIZE_RECORDS, 1);
            put(MEM_CACHE_SIZE_RECORDS, 1);
        }};
        kvStore.createTable(dbName, tableName, TableEngineType.LSM.toString(), properties);
        kvStore.put(dbName, tableName, "key12", "age12".getBytes());

        HttpResponse<String> httpResponse = kvServerHttpClient.sendDropTableRequest(dbName, tableName);
        assertEquals(200, httpResponse.statusCode());
    }

    @Test
    public void testPutEntity() throws IOException, InterruptedException, URISyntaxException {
        String tableName = "test_put_entity2";
        kvStore.createTable(dbName, tableName, TableEngineType.LSM.toString(), new Properties());
        String key = "key12";
        String value = "age12";
        HttpResponse<String> httpResponse = kvServerHttpClient.sendPutEntityRequest(dbName, tableName, key, value);

        assertEquals(200, httpResponse.statusCode());
        String actValue = new String(kvStore.get(dbName, tableName, key));
        assertEquals(value, actValue);
    }

    @Test
    public void testPutEntity2() throws IOException, InterruptedException, URISyntaxException {
        String tableName = "test_put_entity";
        kvStore.createTable(dbName, tableName, TableEngineType.LSM.toString(), new Properties());
        String key = "key12";
        String value = "age12";
        HttpResponse<String> httpResponse = kvServerHttpClient.sendPutEntityRequest(dbName, tableName, key, value.getBytes());

        assertEquals(200, httpResponse.statusCode());
        String actValue = new String(kvStore.get(dbName, tableName, key));
        assertEquals(value, actValue);
    }

    @Test
    public void testPutBatchOfEntities() throws IOException, InterruptedException, URISyntaxException {
        String tableName = "test_put_batch_of_entity";
        kvStore.createTable(dbName, tableName, TableEngineType.LSM.toString(), new Properties());
        ArrayList<KVRecord> records = new ArrayList<>();
        {
            int recCount = 100_000;
            for (int i = 0; i < recCount; i++) {
                String key = "key" + String.format("%06d", i);
                String value = "value" + String.format("%06d", i);
                records.add(new KVRecord(key, value));
            }

        }

        {
            HttpResponse<String> httpResponse = kvServerHttpClient.sendPutBatchOfEntitiesRequest(dbName, tableName, records);
            assertEquals(200, httpResponse.statusCode());
            for (KVRecord record : records) {
                String expValue = new String(record.valueBytes);
                String actValue = new String(kvStore.get(dbName, tableName, record.key));
                assertEquals(expValue, actValue);
            }
        }

    }

    @Test
    public void testRemoveEntity() throws IOException, InterruptedException, URISyntaxException {
        String tableName = "test_remove_entity";
        kvStore.createTable(dbName, tableName, TableEngineType.LSM.toString(), new Properties());
        String key = "key12";
        String value = "age12";
        kvStore.put(dbName, tableName, key, value.getBytes());

        HttpResponse<String> httpResponse = kvServerHttpClient.sendRemoveEntityRequest(dbName, tableName, key);
        System.out.println(httpResponse);
        assertEquals(200, httpResponse.statusCode());
        assertNull(kvStore.get(dbName, tableName, key));
    }

    @Test
    public void testGetEntity() throws IOException, InterruptedException, URISyntaxException {
        String tableName = "test_get_entity";
        kvStore.createTable(dbName, tableName, TableEngineType.LSM.toString(), new Properties());
        String key = "key12";
        String value = "age12";
        kvStore.put(dbName, tableName, key, value.getBytes());

        HttpResponse<byte[]> httpResponse = kvServerHttpClient.sendGetEntityRequest(dbName, tableName, key);
        System.out.println(httpResponse);
        assertEquals(200, httpResponse.statusCode());
        assertEquals(value, new String(httpResponse.body()));
    }

    @Test
    public void testGetEntity2() throws URISyntaxException, IOException, InterruptedException {
        String tableName = "test_get_entity2";
        kvStore.createTable(dbName, tableName, TableEngineType.LSM.toString(), new Properties());
        String key = "key12";

        HttpResponse<byte[]> httpResponse = kvServerHttpClient.sendGetEntityRequest(dbName, tableName, key);
        assertEquals(200, httpResponse.statusCode());
        assertNull(httpResponse.body());
    }

    @Test
    public void testGetAll() throws IOException, InterruptedException, URISyntaxException {
        int recCount = 100_000;
        String tableName = "test_get_all";
        kvStore.createTable(dbName, tableName, TableEngineType.LSM.toString(), new Properties());
        ArrayList<KVRecord> expRecords = new ArrayList<>(recCount);
        for (int i = 0; i < recCount; i++) {
            String key = "key" + String.format("%06d", i);
            String value = "value" + String.format("%06d", i);
            expRecords.add(new KVRecord(key, value));
            kvStore.put(dbName, tableName, key, value.getBytes());
        }
        expRecords.sort(Comparator.comparing(o -> o.key));


        Iterator<KVRecord> allIterator = kvServerHttpClient.getAll(dbName, tableName, 1024);
        List<KVRecord> actRecords = new ArrayList<>(recCount);
        allIterator.forEachRemaining(actRecords::add);

        assertEquals(expRecords, actRecords);
    }

    @Test
    public void testGetRecordsCnt() throws IOException, InterruptedException, URISyntaxException {
        int recCount = 100_000;
        String tableName = "test_get_records_cnt";
        kvStore.createTable(dbName, tableName, TableEngineType.LSM.toString(), new Properties());
        ArrayList<KVRecord> expRecords = new ArrayList<>(recCount);
        for (int i = 0; i < recCount; i++) {
            String key = "key" + String.format("%06d", i);
            String value = "value" + String.format("%06d", i);
            expRecords.add(new KVRecord(key, value));
            kvStore.put(dbName, tableName, key, value.getBytes());
        }
        expRecords.sort(Comparator.comparing(o -> o.key));

        Query query = new Query.QueryBuilder().withNoLimit().withNoMaxBound().withNoMinBound().build();
        int recordsCnt = kvServerHttpClient.getRecordsCnt(dbName, tableName, query);

        assertEquals(recCount, recordsCnt);
    }

    @Test
    public void testOptimizeTable() throws IOException, InterruptedException, URISyntaxException {
        String tableName = "test_optimize";
        Properties properties = new Properties() {{
            put(SPARSE_INDEX_SIZE_RECORDS, 1);
            put(MEM_CACHE_SIZE_RECORDS, 2);
            put(SYNC_WITH_THREAD_FLUSH, true);
        }};
        kvStore.createTable(dbName, tableName, TableEngineType.LSM.toString(), properties);

        kvStore.put(dbName, tableName, "key12", "age12".getBytes());
        kvStore.put(dbName, tableName, "key13", "age14".getBytes());
        kvStore.put(dbName, tableName, "key14", "age14".getBytes());
        kvStore.put(dbName, tableName, "key15", "age15".getBytes());
        kvStore.flushTable(dbName, tableName);

        File tableDir = storageDirPath.resolve(dbName).resolve(tableName).resolve(TableEngineType.LSM.toString()).resolve("data").toFile();
        TestUtils.assertNumberOfSSTables(tableDir, 2);

        HttpResponse<String> httpResponse = kvServerHttpClient.sendOptimizeTableRequest(dbName, tableName);
        assertEquals(200, httpResponse.statusCode());
        TestUtils.assertNumberOfSSTables(tableDir, 1);
    }

    @Test
    public void testFlushTable() throws IOException, InterruptedException, URISyntaxException {
        String tableName = "test_flush";
        Properties properties = new Properties() {{
            put(SPARSE_INDEX_SIZE_RECORDS, 1);
            put(MEM_CACHE_SIZE_RECORDS, 2);
            put(SYNC_WITH_THREAD_FLUSH, true);
        }};
        kvStore.createTable(dbName, tableName, TableEngineType.LSM.toString(), properties);


        kvStore.put(dbName, tableName, "key12", "age12".getBytes());
        kvStore.put(dbName, tableName, "key13", "age14".getBytes());
        kvStore.put(dbName, tableName, "key14", "age14".getBytes());
        kvStore.put(dbName, tableName, "key15", "age16".getBytes());
        File tableDir = storageDirPath.resolve(dbName).resolve(tableName).resolve(TableEngineType.LSM.toString()).resolve("data").toFile();
        TestUtils.assertNumberOfSSTables(tableDir, 1);

        HttpResponse<String> httpResponse = kvServerHttpClient.sendFlushTableRequest(dbName, tableName);
        System.out.println(httpResponse);
        assertEquals(200, httpResponse.statusCode());
        TestUtils.assertNumberOfSSTables(tableDir, 2);
    }

    @Test
    public void testCreateCheckpoint() throws URISyntaxException, IOException, InterruptedException {
        String tableName = "test_checkpoint";
        Path tableDir = storageDirPath.resolve(dbName).resolve(tableName);
        File dataDir = tableDir.resolve(TableEngineType.LSM.toString()).resolve("data").toFile();
        {
            Properties properties = new Properties() {{
                put(SPARSE_INDEX_SIZE_RECORDS, 1);
                put(MEM_CACHE_SIZE_RECORDS, 2);
                put(SYNC_WITH_THREAD_FLUSH, true);
            }};
            kvStore.createTable(dbName, tableName, TableEngineType.LSM.toString(), properties);
        }

        {
            kvStore.put(dbName, tableName, "key12", "age12".getBytes());
            kvStore.put(dbName, tableName, "key13", "age14".getBytes());
            kvStore.put(dbName, tableName, "key14", "age15".getBytes());
            kvStore.put(dbName, tableName, "key15", "age16".getBytes());
            TestUtils.assertSStablesExists(dataDir, "1v1");
            kvServerHttpClient.sendMakeTableCheckpointRequest(dbName, tableName);
            TestUtils.assertSStablesExists(dataDir, "1v1", "2v1");
            TestUtils.assertCheckpointDirContainsLsmSSTables(tableDir.toFile(), "1v1", "2v1");
        }

        {
            kvStore.put(dbName, tableName, "key22", "age12".getBytes());
            kvStore.put(dbName, tableName, "key23", "age14".getBytes());
            kvStore.put(dbName, tableName, "key24", "age15".getBytes());
            kvStore.put(dbName, tableName, "key25", "age16".getBytes());
            TestUtils.assertSStablesExists(dataDir, "1v1", "2v1", "3v1");
            kvServerHttpClient.sendMakeTableCheckpointRequest(dbName, tableName);
            TestUtils.assertSStablesExists(dataDir, "1v1", "2v1", "3v1", "4v1");
            TestUtils.assertCheckpointDirContainsLsmSSTables(tableDir.toFile(), "1v1", "2v1", "3v1", "4v1");
        }
    }

    @Test
    public void testGetDBs() throws IOException, InterruptedException, URISyntaxException {
        String dbName1 = "test_get_db1";
        String dbName2 = "test_get_db2";
        kvStore.createDB(dbName1);
        kvStore.createDB(dbName2);

        HashSet<String> dbs = kvServerHttpClient.sendGetDBsRequest();

        assertTrue(dbs.contains(dbName1.toUpperCase()));
        assertTrue(dbs.contains(dbName2.toUpperCase()));
    }

    @Test
    public void testGetTables() throws IOException, InterruptedException, URISyntaxException {

        String table1 = "test_get_tables1";
        String table2 = "test_get_tables2";
        kvStore.createTable(dbName, table1, TableEngineType.LSM.toString(), new Properties());
        kvStore.createTable(dbName, table2, TableEngineType.LSM.toString(), new Properties());

        HashSet<String> tbls = kvServerHttpClient.sendGetTablesRequest(dbName);
        assertTrue(tbls.contains(table1.toUpperCase()));
        assertTrue(tbls.contains(table2.toUpperCase()));
    }

    @Test
    public void testGetTableParameters() throws IOException, InterruptedException, URISyntaxException {

        String table1 = "test_get_table_parameters";
        kvStore.createTable(dbName, table1, TableEngineType.LSM.toString(), new Properties());

        HttpResponse<String> httpResponse = kvServerHttpClient.sendGetTableParameters(dbName, table1);

        System.out.println(httpResponse);
        System.out.println(httpResponse.body());
        assertEquals(200, httpResponse.statusCode());

        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode jsonNode = objectMapper.readTree(httpResponse.body());
        JsonNode engineParams = jsonNode.get("engine");
        JsonNode tableParams = jsonNode.get("table");
        assertEquals("0.5", engineParams.get(ConfigParams.LSM_BLOOM_FILTER_FALSE_POSITIVE_RATE.toUpperCase()).textValue());
        assertEquals("128", engineParams.get(ConfigParams.LSM_SPARSE_INDEX_SIZE_RECORDS.toUpperCase()).textValue());
        assertEquals("100000", engineParams.get(ConfigParams.LSM_MEM_CACHE_SIZE.toUpperCase()).textValue());
        assertEquals("10", tableParams.get(ConfigParams.KV_TABLE_COMMIT_LOG_PARALLELISM.toUpperCase()).textValue());
    }

    @AfterAll
    public static void stopServer() {
        kvHttpServer.stop();
    }

    private void assertDbExists(String dbName) {
        ArrayList<String> databasesList = kvStore.getDatabasesList();
        assertTrue(databasesList.contains(dbName.toUpperCase()));
    }

    private void assertDbNotExists(String dbName) {
        ArrayList<String> databasesList = kvStore.getDatabasesList();
        assertFalse(databasesList.contains(dbName.toUpperCase()));
    }

    private void assertException(HttpResponse<String> httpResponse, Class<?> clazz) throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode jsonNode = mapper.readTree(httpResponse.body());
        JsonNode error = jsonNode.get("errorClass");
        assertEquals(clazz.getName(), error.textValue());

    }

    private void assertResponseData(HttpResponse<String> httpResponse, String key, String value) throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode jsonNode = mapper.readTree(httpResponse.body());
        String keyAct = jsonNode.get("key").textValue();
        String valueAct = jsonNode.get("value").textValue();
        assertEquals(key, keyAct);
        assertEquals(value, valueAct);
    }


}
