package com.keks.kv_storage.recovery;

import com.keks.kv_storage.KVStore;
import com.keks.kv_storage.client.KVServerHttpClient;
import com.keks.kv_storage.conf.ConfigParams;
import com.keks.kv_storage.conf.TableEngineType;
import com.keks.kv_storage.utils.UnCheckedBiConsumer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import utils.EmbeddedKvStore;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;


class RecoveryManagerTest {


    @Test
    public void testNoRecover(@TempDir Path dir) throws IOException {
        createJournalFile(dir, JournalEvent.END);
        RecoveryJournal recoveryJournal = new RecoveryJournal(new File(dir.toFile(), RecoveryJournal.RECOVERY_JOURNAL_FILENAME));
        RecoveryManager recoveryManager = new RecoveryManager("test1", dir.toFile(), 10, recoveryJournal);
        assertFalse(recoveryManager.needRecovery());
    }


    // Recover After Running
    @Test
    public void test1(@TempDir Path dir) throws IOException {
        createJournalFile(dir, JournalEvent.RUNNING);
        createDataDir(dir, "data1.db");
        createCheckpoint(dir, "data_check.db");
        createCommitLogDir(dir, "commit1.db");
        RecoveryJournal recoveryJournal = new RecoveryJournal(new File(dir.toFile(), RecoveryJournal.RECOVERY_JOURNAL_FILENAME));
        RecoveryManager recoveryManager = new RecoveryManager("test1", dir.toFile(), 10, recoveryJournal);
        assertTrue(recoveryManager.needRecovery());

        boolean needToLoadData = recoveryManager.prepareDataForRecoveryIfNeeded();
        assertTrue(needToLoadData);

        assertDataDirContains(dir, "data_check.db");
    }

    // Recover After RECOVERING_DATA_FROM_CHECKPOINT
    @Test
    public void test2(@TempDir Path dir1, @TempDir Path dir2) throws IOException {
        // data dir exists
        {
            createJournalFile(dir1, JournalEvent.RECOVERING_DATA_FROM_CHECKPOINT);
            createDataDir(dir1, "data1.db");
            createCheckpoint(dir1, "data_check.db");
            createCommitLogDir(dir1, "commit1.db");
            RecoveryJournal recoveryJournal = new RecoveryJournal(new File(dir1.toFile(), RecoveryJournal.RECOVERY_JOURNAL_FILENAME));
            RecoveryManager recoveryManager = new RecoveryManager("test1", dir1.toFile(), 10, recoveryJournal);

            assertTrue(recoveryManager.needRecovery());
            boolean needToLoadData = recoveryManager.prepareDataForRecoveryIfNeeded();
            assertTrue(needToLoadData);
            assertDataDirContains(dir1, "data_check.db");
        }
        // data dir doesn't exist
        {
            createJournalFile(dir2, JournalEvent.RECOVERING_DATA_FROM_CHECKPOINT);
            createCheckpoint(dir2, "data_check.db");
            createCommitLogDir(dir2, "commit1.db");
            RecoveryJournal recoveryJournal = new RecoveryJournal(new File(dir2.toFile(), RecoveryJournal.RECOVERY_JOURNAL_FILENAME));
            RecoveryManager recoveryManager = new RecoveryManager("test1", dir2.toFile(), 10, recoveryJournal);

            assertTrue(recoveryManager.needRecovery());
            boolean needToLoadData = recoveryManager.prepareDataForRecoveryIfNeeded();
            assertTrue(needToLoadData);
            assertDataDirContains(dir2, "data_check.db");
        }
    }


    // Recover After CREATING_CHECKPOINT
    @Test
    public void test8(@TempDir Path dir1, @TempDir Path dir2) throws IOException {

        {
            createJournalFile(dir1, JournalEvent.CREATING_CHECKPOINT);
            createDataDir(dir1, "data1.db");
            createCheckpoint(dir1, "data_check.db");
            createCommitLogDir(dir1, "commit1.db");
            RecoveryJournal recoveryJournal = new RecoveryJournal(new File(dir1.toFile(), RecoveryJournal.RECOVERY_JOURNAL_FILENAME));
            RecoveryManager recoveryManager = new RecoveryManager("test1", dir1.toFile(), 10, recoveryJournal);

            assertTrue(recoveryManager.needRecovery());
            boolean needToLoadData = recoveryManager.prepareDataForRecoveryIfNeeded();
            assertFalse(needToLoadData);
            assertCheckpointDirContains(dir1, "data1.db");
            assertCommitLogDirDoesNotExist(dir1);
        }

        {
            createJournalFile(dir2, JournalEvent.CREATING_CHECKPOINT);
            createDataDir(dir2, "data1.db");
            createCommitLogDir(dir2, "commit1.db");
            RecoveryJournal recoveryJournal = new RecoveryJournal(new File(dir2.toFile(), RecoveryJournal.RECOVERY_JOURNAL_FILENAME));
            RecoveryManager recoveryManager = new RecoveryManager("test1", dir2.toFile(), 10, recoveryJournal);

            assertTrue(recoveryManager.needRecovery());
            boolean needToLoadData = recoveryManager.prepareDataForRecoveryIfNeeded();
            assertFalse(needToLoadData);
            assertCheckpointDirContains(dir2, "data1.db");
            assertCommitLogDirDoesNotExist(dir2);
        }
    }

    @Test
    public void testRecoverLsmBplus(@TempDir Path dir) throws IOException, URISyntaxException, InterruptedException {
        int httpPort = 8088;


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
            {
                EmbeddedKvStore embeddedKvStore = new EmbeddedKvStore(dir.toFile(), httpPort, 5, 8089);
                embeddedKvStore.start();

                KVServerHttpClient client = new KVServerHttpClient("localhost", httpPort);
                {
                    client.sendCreateDBRequest(dbName1);
                    client.sendCreateTableRequest(dbName1, tableName1, TableEngineType.LSM, new HashMap<>() {{
                        put(ConfigParams.LSM_SPARSE_INDEX_SIZE_RECORDS, 10);
                        put(ConfigParams.LSM_MEM_CACHE_SIZE, 10);
                        put(ConfigParams.LSM_BLOOM_FILTER_FALSE_POSITIVE_RATE, 0.5);
                    }});
                    client.sendPutEntityRequest(dbName1, tableName1, "key1" + keyPostfix1, "haha1");
                    client.sendPutEntityRequest(dbName1, tableName1, "key2" + keyPostfix1, "haha2");
                }
                {
                    client.sendCreateTableRequest(dbName1, tableName2, TableEngineType.BPLUS, new HashMap<>() {{
                        put(ConfigParams.LSM_SPARSE_INDEX_SIZE_RECORDS, 10);
                        put(ConfigParams.LSM_MEM_CACHE_SIZE, 10);
                        put(ConfigParams.LSM_BLOOM_FILTER_FALSE_POSITIVE_RATE, 0.5);
                    }});
                    client.sendPutEntityRequest(dbName1, tableName2, "key1" + keyPostfix2, "haha1");
                    client.sendMakeTableCheckpointRequest(dbName1, tableName2);
                    client.sendPutEntityRequest(dbName1, tableName2, "key2" + keyPostfix2, "haha2");
                }
                {
                    client.sendCreateDBRequest(dbName2);
                    client.sendCreateTableRequest(dbName2, tableName3, TableEngineType.LSM, new HashMap<>() {{
                        put(ConfigParams.LSM_SPARSE_INDEX_SIZE_RECORDS, 10);
                        put(ConfigParams.LSM_MEM_CACHE_SIZE, 10);
                        put(ConfigParams.LSM_BLOOM_FILTER_FALSE_POSITIVE_RATE, 0.5);
                    }});
                    client.sendPutEntityRequest(dbName2, tableName3, "key1" + keyPostfix3, "haha1");
                    client.sendMakeTableCheckpointRequest(dbName2, tableName3);
                    client.sendPutEntityRequest(dbName2, tableName3, "key2" + keyPostfix3, "haha2");
                }
                {
                    client.sendCreateDBRequest(dbName2);
                    client.sendCreateTableRequest(dbName2, tableName4, TableEngineType.BPLUS, new HashMap<>() {{
                        put(ConfigParams.LSM_SPARSE_INDEX_SIZE_RECORDS, 10);
                        put(ConfigParams.LSM_MEM_CACHE_SIZE, 10);
                        put(ConfigParams.LSM_BLOOM_FILTER_FALSE_POSITIVE_RATE, 0.5);
                    }});
                    client.sendPutEntityRequest(dbName2, tableName4, "key1" + keyPostfix4, "haha1");
                    client.sendMakeTableCheckpointRequest(dbName2, tableName4);
                    client.sendPutEntityRequest(dbName2, tableName4, "key2" + keyPostfix4, "haha2");
                }
                embeddedKvStore.shutdownForce();
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

        {
            {
                EmbeddedKvStore embeddedKvStore = new EmbeddedKvStore(dir.toFile(), httpPort, 5, 8089);
                embeddedKvStore.start();

                KVServerHttpClient client = new KVServerHttpClient("localhost", httpPort);
                {
                    client.sendPutEntityRequest(dbName1, tableName1, "key3" + keyPostfix1, "haha1");
                    client.sendMakeTableCheckpointRequest(dbName1, tableName1);
                    client.sendPutEntityRequest(dbName1, tableName1, "key4" + keyPostfix1, "haha2");
                }
                {
                    client.sendPutEntityRequest(dbName1, tableName2, "key3" + keyPostfix2, "haha1");
                    client.sendMakeTableCheckpointRequest(dbName1, tableName2);
                    client.sendPutEntityRequest(dbName1, tableName2, "key4" + keyPostfix2, "haha2");
                }
                {
                    client.sendPutEntityRequest(dbName2, tableName3, "key3" + keyPostfix3, "haha1");
                    client.sendMakeTableCheckpointRequest(dbName2, tableName3);
                    client.sendPutEntityRequest(dbName2, tableName3, "key4" + keyPostfix3, "haha2");
                }
                {
                    client.sendPutEntityRequest(dbName2, tableName4, "key3" + keyPostfix4, "haha1");
                    client.sendMakeTableCheckpointRequest(dbName2, tableName4);
                    client.sendPutEntityRequest(dbName2, tableName4, "key4" + keyPostfix4, "haha2");
                }
                embeddedKvStore.shutdownForce();
            }

            {
                KVStore kvStore = new KVStore(dir.toFile());
                {
                    assertEquals("haha1", new String(kvStore.get(dbName1, tableName1, "key3" + keyPostfix1)));
                    assertEquals("haha2", new String(kvStore.get(dbName1, tableName1, "key4" + keyPostfix1)));
                }
                {
                    assertEquals("haha1", new String(kvStore.get(dbName1, tableName2, "key3" + keyPostfix2)));
                    assertEquals("haha2", new String(kvStore.get(dbName1, tableName2, "key4" + keyPostfix2)));
                }
                {
                    assertEquals("haha1", new String(kvStore.get(dbName2, tableName3, "key3" + keyPostfix3)));
                    assertEquals("haha2", new String(kvStore.get(dbName2, tableName3, "key4" + keyPostfix3)));
                }
                {
                    assertEquals("haha1", new String(kvStore.get(dbName2, tableName4, "key3" + keyPostfix4)));
                    assertEquals("haha2", new String(kvStore.get(dbName2, tableName4, "key4" + keyPostfix4)));
                }
                kvStore.close();
            }
        }
    }

    private void createJournalFile(Path dir, JournalEvent journalEvent) throws IOException {
        RecoveryJournal recoveryJournal = RecoveryJournal.create(dir.toFile());
        recoveryJournal.writeEvent(journalEvent);
        recoveryJournal.close();
    }

    private void createDataDir(Path dir, String fileName) throws IOException {
        File dirFile = dir.resolve(RecoveryManager.DATA_DIR_NAME).toFile();
        assertFalse(dirFile.exists());
        dirFile.mkdir();
        new File(dirFile, fileName).createNewFile();
    }

    private void createCheckpoint(Path dir, String fileName) throws IOException {
        File dirFile = dir.resolve(RecoveryManager.CHECKPOINT_DIR_NAME).toFile();
        assertFalse(dirFile.exists());
        dirFile.mkdir();
        new File(dirFile, fileName).createNewFile();
    }

    private void createCommitLogDir(Path dir, String fileName) throws IOException {
        File dirFile = dir.resolve(RecoveryManager.COMMIT_LOG_DIR_NAME).toFile();
        assertFalse(dirFile.exists());
        dirFile.mkdir();
        new File(dirFile, fileName).createNewFile();
    }


    public void assertDataDirContains(Path dir, String file) {
        assertTrue(dir.resolve(RecoveryManager.DATA_DIR_NAME).resolve(file).toFile().exists());
    }

    public void assertCheckpointDirContains(Path dir, String file) {
        assertTrue(dir.resolve(RecoveryManager.CHECKPOINT_DIR_NAME).resolve(file).toFile().exists());
    }

    public void assertCommitLogDirDoesNotExist(Path dir) {
        File dirFile = dir.resolve(RecoveryManager.COMMIT_LOG_DIR_NAME).toFile();
        assertFalse(dirFile.exists());
    }

    public void assertCheckpointDirExists(Path dir) {
        File dirFile = dir.resolve(RecoveryManager.CHECKPOINT_DIR_NAME).toFile();
        assertTrue(dirFile.exists());
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

}
