package perf_test.concurrent;

import com.keks.kv_storage.KVStore;
import com.keks.kv_storage.conf.ConfigParams;
import com.keks.kv_storage.conf.TableEngineType;
import com.keks.kv_storage.server.http.KVHttpServer;
import com.keks.kv_storage.client.KVServerHttpClient;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.io.TempDir;
import utils.TestUtils;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

class KVHttpServerConcurrentTest {

    private static File storageDir;
    private static KVServerHttpClient kvServerHttpClient;
    private static KVHttpServer kvHttpServer;
    private static KVStore kvStore;

    @BeforeAll
    public static void startServer(@TempDir Path dir) throws IOException {
        System.setProperty("sun.net.httpserver.maxIdleConnections", "1000");
        storageDir = dir.toFile();
        int defaultNumberOfKeysInBlockIndex = 100;
        int defaultMaxNumberOfRecordsInMemory = 1_000;
        int serverPort = 8764;
        String serverHost = "localhost";
        kvServerHttpClient = new KVServerHttpClient(serverHost, serverPort);
        kvStore = new KVStore(storageDir);
        kvHttpServer = new KVHttpServer(serverPort, kvStore, 1, 10);
        kvHttpServer.start();
    }

//    @Test
    public void testKVHttpServer() throws IOException, InterruptedException, ExecutionException, TimeoutException, URISyntaxException {
        int taskCount = 1_000_000;
        int numberOfThreads = 50;
        int defaultNumberOfKeysInBlockIndex = 100;
        int defaultMaxNumberOfRecordsInMemory = 100_000;
        String dbName = "test_db";
        String tableName = "test3";
        String engine = "lsm";
        int serverPort = 8764;
        String serverHost = "localhost";
        KVServerHttpClient kvServerHttpClient1 = new KVServerHttpClient(serverHost, serverPort);
        kvServerHttpClient1.sendDropTableRequest(dbName, tableName);
        kvServerHttpClient1.sendCreateTableRequest(dbName, tableName, TableEngineType.LSM, new HashMap<>() {{
            put(ConfigParams.LSM_SPARSE_INDEX_SIZE, defaultNumberOfKeysInBlockIndex);
            put(ConfigParams.LSM_MEM_CACHE_SIZE, defaultMaxNumberOfRecordsInMemory);
            put(ConfigParams.LSM_BLOOM_FILTER_FALSE_POSITIVE_RATE, 0.1);
        }});


        Function<Integer, String> func1 = i -> {
            String key = "key" + i;
            String value = "value" + i;
            try {
//                System.out.println("send: " + i);
//                Thread.sleep(10);
                kvServerHttpClient1.sendPutEntityRequest(dbName, tableName, key, value);
//                System.out.println("received: " + i);
            } catch (IOException e) {
                e.printStackTrace();
                throw new RuntimeException();
            } catch (URISyntaxException | InterruptedException e) {
                e.printStackTrace();
            }
            return "";
        };
        Instant start = Instant.now();
        TestUtils.runConcurrentTest(taskCount, 10000, 1, func1, numberOfThreads);
        Instant finish = Instant.now();
        Duration between = Duration.between(start, finish);


//        Function<Integer, String> func2 = i -> {
//            String key = "key" + i;
//            String value = "value" + i;
//            try {
//                if (i % 3 == 0) {
//                    String expBody = "{\"key\":\"" + key + "\",\"value\":null}";
//                    assertEquals(expBody, kvServerHttpClient.sendGetEntityRequest(tableName, key));
//                } else {
//                    String expBody = "{\"key\":\"" + key + "\",\"value\":\"" + value + "\"}";
//                    assertEquals(expBody, kvServerHttpClient.sendGetEntityRequest(tableName, key));
//                }
//            } catch (IOException e) {
//                e.printStackTrace();
//                throw new RuntimeException();
//            } catch (URISyntaxException | InterruptedException e) {
//                e.printStackTrace();
//            }
//            return "";
//        };
//        TestUtils.runConcurrentTest(taskCount, 10000,100, func2, 32);
//
//        kvServerHttpClient.sendFlushTableRequest(tableName);
//        kvServerHttpClient.sendFullCompactTableRequest(tableName);
//        TestUtils.runConcurrentTest(taskCount, 10000,100, func2, 32);

    }

    @AfterAll
    public static void stopServer() {
        kvHttpServer.stop();
    }

}