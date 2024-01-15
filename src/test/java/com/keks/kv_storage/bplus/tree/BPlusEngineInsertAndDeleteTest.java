package com.keks.kv_storage.bplus.tree;

import com.keks.kv_storage.bplus.BPlusEngine;
import com.keks.kv_storage.bplus.buffer.PageBuffer;
import com.keks.kv_storage.bplus.TableName;
import com.keks.kv_storage.bplus.conf.BPlusConf;
import com.keks.kv_storage.record.KVRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;


class BPlusEngineInsertAndDeleteTest {

    private static final TableName tableName = new TableName("test");


    @Test
    public void test1(@TempDir Path dir) throws IOException {
        int treeOrder = 3;
        BPlusConf btreeParams = new BPlusConf(treeOrder, 10, 10,40_000_00);
        BPlusEngine kvTable = BPlusEngine.createNewTable("", dir.toFile(), btreeParams);

        System.out.println("Add1: key0000 : valuekey0000");
        kvTable.addWithWriteLock(new KVRecord("key0000", "valuekey0000".getBytes()));
        kvTable.printTree();
        System.out.println();

        System.out.println("Add1: key0001 : valuekey0001");
        kvTable.addWithWriteLock(new KVRecord("key0001", "valuekey0001".getBytes()));
        kvTable.printTree();
        System.out.println();

        System.out.println("Get2: key0000");
        kvTable.get("key0000");
        kvTable.printTree();
        System.out.println();

        System.out.println("Get2: key0001");
        kvTable.get("key0001");
        kvTable.printTree();
        System.out.println();

        System.out.println("Add1: key0002 : valuekey0002");
        kvTable.addWithWriteLock(new KVRecord("key0002", "valuekey0002".getBytes()));
        kvTable.printTree();
        System.out.println();

        System.out.println("Add3: key0000 : value_new0");
        kvTable.addWithWriteLock(new KVRecord("key0000", "value_new0".getBytes()));
        kvTable.printTree();
        System.out.println();

        System.out.println("Get2: key0002");
        kvTable.get("key0002");
        kvTable.printTree();
        System.out.println();

        System.out.println("Add3: key0002 : value_new2");
        kvTable.addWithWriteLock(new KVRecord("key0002", "value_new2".getBytes()));
        kvTable.printTree();
        System.out.println();

        System.out.println("Get4: key0000");
        kvTable.get("key0000");
        kvTable.printTree();
        System.out.println();

        System.out.println("Get4: key0002");
        kvTable.get("key0002");
        kvTable.printTree();
        System.out.println();

        System.out.println("Delete5: key0000");
        kvTable.deleteWithWriteLock("key0000");
        kvTable.printTree();
        System.out.println();

        System.out.println("Delete5: key0002");
        kvTable.deleteWithWriteLock("key0002");
        kvTable.printTree();
        System.out.println();

        System.out.println("Get6: key0000");
        kvTable.get("key0000");
        kvTable.printTree();
        System.out.println();

        System.out.println("Get6: key0002");
        kvTable.get("key0002");
        kvTable.printTree();
        System.out.println();

        System.out.println("Add1: key0003 : valuekey0003");
        kvTable.addWithWriteLock(new KVRecord("key0003", "valuekey0003".getBytes()));
        kvTable.printTree();
        System.out.println();

        System.out.println("Add7: key0000 : value_new20");
        kvTable.addWithWriteLock(new KVRecord("key0000", "value_new20".getBytes()));
        kvTable.printTree();
        System.out.println();

        System.out.println("Get2: key0003");
        kvTable.get("key0003");
        kvTable.printTree();
        System.out.println();

        System.out.println("Get8: key0000");
        kvTable.get("key0000");
        kvTable.printTree();
        System.out.println();

        System.out.println("Add7: key0003 : value_new23");
        kvTable.addWithWriteLock(new KVRecord("key0003", "value_new23".getBytes()));
        kvTable.printTree();
        System.out.println();

        System.out.println("Delete9: key0000");
        kvTable.deleteWithWriteLock("key0000");
        kvTable.printTree();
        System.out.println();

        System.out.println("Get8: key0003");
        kvTable.get("key0003");
        kvTable.printTree();
        System.out.println();

        System.out.println("Get10: key0000");
        kvTable.get("key0000");
        kvTable.printTree();
        System.out.println();

        System.out.println("Add1: key0004 : valuekey0004");
        kvTable.addWithWriteLock(new KVRecord("key0004", "valuekey0004".getBytes()));
        kvTable.printTree();
        System.out.println();

        System.out.println("Add1: key0005 : valuekey0005");
        kvTable.addWithWriteLock(new KVRecord("key0005", "valuekey0005".getBytes()));
        kvTable.printTree();
        System.out.println();

        System.out.println("Get2: key0004");
        kvTable.get("key0004");
        kvTable.printTree();
        System.out.println();

        System.out.println("Get2: key0005");
        kvTable.get("key0005");
        kvTable.printTree();
        System.out.println();

        System.out.println("Add3: key0004 : value_new4");
        kvTable.addWithWriteLock(new KVRecord("key0004", "value_new4".getBytes()));
        kvTable.printTree();
        System.out.println();

        System.out.println("Add1: key0006 : valuekey0006");
        kvTable.addWithWriteLock(new KVRecord("key0006", "valuekey0006".getBytes()));
        kvTable.printTree();
        System.out.println();

        System.out.println("Get4: key0004");
        kvTable.get("key0004");
        kvTable.printTree();
        System.out.println();

        System.out.println("Get2: key0006");
        kvTable.get("key0006");
        kvTable.printTree();
        System.out.println();

        System.out.println("Add3: key0006 : value_new6");
        kvTable.addWithWriteLock(new KVRecord("key0006", "value_new6".getBytes()));
        kvTable.printTree();
        System.out.println();

        System.out.println("Delete5: key0004");
        kvTable.deleteWithWriteLock("key0004");
        kvTable.printTree();
        System.out.println();

        System.out.println("Get4: key0006");
        kvTable.get("key0006");
        kvTable.printTree();
        System.out.println();

        System.out.println("Get6: key0004");
        kvTable.get("key0004");
        kvTable.printTree();
        System.out.println();

        System.out.println("Delete5: key0006");
        kvTable.deleteWithWriteLock("key0006");
        kvTable.printTree();
        System.out.println();

        System.out.println("Delete9: key0004");
        kvTable.deleteWithWriteLock("key0004");
        kvTable.printTree();
        System.out.println();

        System.out.println("Get6: key0006");
        kvTable.get("key0006");
        kvTable.printTree();
        System.out.println();

        System.out.println("Get10: key0004");
        kvTable.get("key0004");
        kvTable.printTree();
        System.out.println();

        System.out.println("Add1: key0007 : valuekey0007");
        kvTable.addWithWriteLock(new KVRecord("key0007", "valuekey0007".getBytes()));
        kvTable.printTree();
        System.out.println();

    }

    @ParameterizedTest
    @ValueSource(ints = {3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 31, 100, 201})
//    @ValueSource(ints = {100})
//    @RepeatedTest(1000)
    public void test13(int treeOrder, @TempDir Path dir) throws IOException, ExecutionException, InterruptedException, TimeoutException {
//    public void test13(@TempDir Path dir) throws IOException, ExecutionException, InterruptedException, TimeoutException {
//        int treeOrder = 3;
        BPlusConf btreeParams = new BPlusConf(treeOrder, 10, 10,40_000_000);
        BPlusEngine kvTable = BPlusEngine.createNewTable("", dir.toFile(), btreeParams);

        int records = treeOrder * 1_000;
        System.out.println(records);
        AtomicInteger cnt = new AtomicInteger(0);
        Function<Integer, String> func = i -> {
//            reentrantLock.lock();
//            System.out.println("Thread: " + Thread.currentThread().getId());
            String key = "key" + String.format("%04d", i);
            try {

                String value = "value" + key;
//                System.out.println("Add1: " + key + " : " + value);
                kvTable.addWithWriteLock(new KVRecord(key, value.getBytes()));

//                System.out.println("Get2: " + key);
                assertEquals(value, new String(kvTable.get(key).valueBytes));

                if (i % 2 == 0) {
                    value = "value_new" + i;
//                    System.out.println("Add3: " + key + " : " + value);
                    kvTable.addWithWriteLock(new KVRecord(key, value.getBytes()));
//                    System.out.println("Get4: " + key);
                    assertEquals(value, new String(kvTable.get(key).valueBytes));
                }
                if (i % 2 == 0) {
//                    System.out.println("Delete5: " + key);
                    kvTable.deleteWithWriteLock(key);
//                    System.out.println("Get6: " + key);
                    assertNull(kvTable.get(key));
                }

                if (i % 3 == 0) {
                    value = "value_new2" + i;
//                    System.out.println("Add7: " + key + " : " + value);
                    kvTable.addWithWriteLock(new KVRecord(key, value.getBytes()));
//                    System.out.println("Get8: " + key);
                    assertEquals(value, new String(kvTable.get(key).valueBytes));
                }
                if (i % 4 == 0) {
//                    System.out.println("Delete9: " + key);
                    kvTable.deleteWithWriteLock(key);
//                    System.out.println("Get10: " + key);
                    assertNull(kvTable.get(key));
                }
//                kvTable.printTree();
//                System.out.println();
//                reentrantLock.unlock();
                return "";
            } catch (Throwable e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            } finally {
                int ccc = cnt.incrementAndGet();
                if (ccc % 10_000 == 0) System.err.println(ccc);
            }
        };



        runConcurrentTest(records, 300, 100, func, 10);

//        System.out.println(myBuffer.addCnt.get());
//        System.out.println(myBuffer.updateCnt.get());
        // 13351436
        // 12935464

        // 6149667
        // 5954146

    }


    public static void runConcurrentTest(int taskCount,
                                         int threadPoolAwaitTimeoutSec,
                                         int taskAwaitTimeoutSec,
                                         Function<Integer, String> function,
                                         int numberOfThreads) throws InterruptedException, ExecutionException, TimeoutException {
        ExecutorService executor = Executors.newFixedThreadPool(numberOfThreads);
        List<Future<?>> futures = new ArrayList<>(taskCount);
        for (int i = 0; i < taskCount; i++) {
            int y = i;
            Future<?> future1 = executor.submit(() -> function.apply(y));
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