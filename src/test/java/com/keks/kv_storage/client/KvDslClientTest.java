package com.keks.kv_storage.client;

import com.keks.kv_storage.record.KVRecord;
import com.keks.kv_storage.KVStore;
import com.keks.kv_storage.conf.ConfigParams;
import com.keks.kv_storage.conf.TableEngineType;
import com.keks.kv_storage.server.http.KVHttpServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;

import static org.junit.jupiter.api.Assertions.assertEquals;


class KvDslClientTest {

    private static File storageDir;
    private static Path storageDirPath;
    private static KVServerHttpClient kvServerHttpClient;
    private static KVHttpServer kvHttpServer;
    private static KVStore kvStore;


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
    }

    @Test
    public void test1() throws URISyntaxException, IOException, InterruptedException {
        int recCount = 100_000;
        String dbName = "test_db";
        String tblName = "test_tbl";
        int serverPort = 8866;
        String serverHost = "localhost";
        KvDslClient kvDslClient = new KvDslClient(serverHost, serverPort);

        {
            kvDslClient.createDb(dbName);
            kvDslClient.createTable(dbName,
                    tblName,
                    TableEngineType.LSM,
                    new HashMap<>() {
                        {
                            put(ConfigParams.LSM_SPARSE_INDEX_SIZE_RECORDS, 128);
                            put(ConfigParams.LSM_MEM_CACHE_SIZE, 10_000);
                            put(ConfigParams.LSM_BLOOM_FILTER_FALSE_POSITIVE_RATE, 0.1);
                        }
                    });


            ArrayList<KVRecord> records = new ArrayList<>();
            {
                for (int i = 0; i < recCount; i++) {
                    String key = "key" + String.format("%06d", i);
                    String value = "value" + String.format("%06d", i);
                    records.add(new KVRecord(key, value));
                }
            }


            kvDslClient
                    .writeTable(dbName, tblName)
                    .write(records);
        }


        {
            ArrayList<KVRecord> resRec = kvDslClient.readTable(dbName, tblName).getRecordsCollect();
            {
                for (int i = 0; i < recCount; i++) {
                    String key = "key" + String.format("%06d", i);
                    assertEquals(key, resRec.get(i).key);
                }
            }



        }


    }

    @AfterAll
    public static void stopServer() {
        kvHttpServer.stop();
    }

}