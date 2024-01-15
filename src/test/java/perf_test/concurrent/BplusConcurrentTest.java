package perf_test.concurrent;

import com.keks.kv_storage.bplus.buffer.PageBuffer;
import com.keks.kv_storage.bplus.conf.BPlusConf;
import com.keks.kv_storage.record.KVRecord;
import com.keks.kv_storage.bplus.BPlusEngine;
import com.keks.kv_storage.bplus.tree.node.key.TreeKeyNode;
import com.keks.kv_storage.bplus.tree.node.leaf.TreeLeafNode;
import com.keks.kv_storage.utils.Time;
import com.keks.kv_storage.utils.UnCheckedConsumer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import utils.TestUtils;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;


public class BplusConcurrentTest {


    //    @Test
//    public void test3Put(@TempDir Path dir) throws Exception {
//        BufferNew myBuffer = new BufferNew(50_000);
//        int numberOfThreads = 200;
//        BPlusEngineConf bPlusEngineConf = new BPlusEngineConf(400, 1022, 1022);
//        BPlusKVTable bplusTable = BPlusKVTable.createNewTable(dir.toFile(), bPlusEngineConf, myBuffer);
////PT48.290377S, 1
//        // PT11.797962S, 2
//        //PT12.780693S, 5
//        //PT13.216099S, 10
//        //PT12.401958S, 50
////        int records = 1_000_000; // PT1M0.367644S
////        int records = 300_000;
//        int taskCount = 2_000_000;
//
//        AtomicInteger cnt = new AtomicInteger(0);
//        UnCheckedConsumer<Integer, IOException> func = i -> {
//            String key = "key" + i;
//            String value = "value" + i;
//            try {
//
//                bplusTable.put(new KVRecord(key, value.getBytes()));
////
////                assertEquals(value, new String(kvTable.get(key).valueBytes));
////
////                if (i % 2 == 0) {
////                    value = "value";
////                    kvTable.addWithWriteLock(new KVRecord(key, value.getBytes()));
////                    assertEquals(value, new String(kvTable.get(key).valueBytes));
////                }
////
////                if (i % 3 == 0) {
////                    kvTable.deleteWithWriteLock(key);
////
////                }
//            } catch (Throwable t) {
//                System.out.println("Failed on key: " + key);
//                t.printStackTrace();
//                System.exit(1);
//            } finally {
//                int ccc = cnt.incrementAndGet();
//                if (ccc % 100_000 == 0) System.err.println(ccc);
//            }
//
//        };
//
//        Time.withTime("run", () ->
//                TestUtils.runConcurrentTest(taskCount, 10000, 1, func, numberOfThreads)
//        );
//
//    }
//
    @Test
//    @RepeatedTest(10)
    public void test3PutAndGet(@TempDir Path dir) throws Exception {
        int numberOfThreads = 200;
        BPlusConf bPlusConf = new BPlusConf(400, 1022, 1022,800_000_000);
        BPlusEngine bplusTable = BPlusEngine.createNewTable("", dir.toFile(), bPlusConf);
        int taskCount = 10_000_000;

        System.out.println("Running");
        {
            AtomicInteger cntPut = new AtomicInteger(0);
            UnCheckedConsumer<Integer, IOException> funcPut = i -> {
                try {


//                String key = "key" + i;
                    String key = "" + i;
                    String value = "value" + i;
//                printThread("Adding key: " + key);
                    bplusTable.put(new KVRecord(key, value.getBytes()));
//                printThread("Added key: " + key);
                    if (i % 2 == 0) {
//                    printThread("removing key: " + key);
                        bplusTable.remove(key);
//                    printThread("removed key: " + key);
                    }
                    int ccc = cntPut.incrementAndGet();
                    if (ccc % 100_000 == 0) System.err.println(ccc);
                } catch (Throwable e) {
                    e.printStackTrace();
                    System.exit(1);
                    throw new RuntimeException("");
                }

            };

            Time.withTime("put", () ->
                    TestUtils.runConcurrentTest(taskCount, 10000, 1, funcPut, numberOfThreads)
            );

            System.out.println("victim steps to find: " + PageBuffer.victimsStepsToFind.mean(TimeUnit.SECONDS));
            System.out.println("Update cache tries: " + PageBuffer.cnt1.get());
            System.out.println("Try to update: " + PageBuffer.cnt2.get());
            System.out.println("Update: " + PageBuffer.cnt3.get());

        }
        int i1 = TreeKeyNode.getCnt.get();
        int i2 = TreeKeyNode.missedCnt.get();
        int i3 = TreeLeafNode.getCnt.get();
        int i4 = TreeLeafNode.missedCnt.get();
        System.out.println("KeyNode get: " + i1);
        System.out.println("KeyNode missed: " + i2);
        System.out.println("LeafNode get: " + i3);
        System.out.println("LeafNode missed: " + i4);

//        bplusTable.calcNodes();

//        bplusTable.flushAllForce();

        ArrayList<Integer> integers = new ArrayList<>();
        for (int i = 0; i < taskCount; i++) {
            integers.add(i);
        }
        Collections.shuffle(integers);

//        Timer timer321 = BPlusKVTable.registry.timer("timer321");
        Time.mes = true;
        {
            AtomicInteger cntGet = new AtomicInteger(0);
            UnCheckedConsumer<Integer, IOException> funcGet = a -> {
//                Time.withTimer(timer321, () -> {
                int i = integers.get(a);
//                    String key = "key" + i;
                String key = "" + i;
                try {
                    String value = "value" + i;
                    if (i % 2 == 0) {
                        assertNull(bplusTable.get(key));
                    } else {
                        assertEquals(value, new String(bplusTable.get(key).valueBytes));
                    }

                    int ccc = cntGet.incrementAndGet();
                    if (ccc % 100_000 == 0) System.err.println(ccc);
                } catch (Throwable t) {
                    System.out.println("Failed on key: " + key);
                    t.printStackTrace();
                    System.exit(1);
                }
//                });
            };

            Time.withTime("get", () ->
                    runConcurrentTest(taskCount, 10000, 1, funcGet, numberOfThreads)
            );
//            System.out.println(TestUtils.timer4.totalTime(TimeUnit.SECONDS));
//            System.out.println(timer321.totalTime(TimeUnit.SECONDS));
//            System.out.println(timerSubmit.totalTime(TimeUnit.SECONDS));
//            System.out.println(timerShutDown.totalTime(TimeUnit.SECONDS));
//            System.out.println(timerAssert.totalTime(TimeUnit.SECONDS));
//            System.out.println("\nrecurFindTimer");
//            System.out.println(BPlusKVTable.recurFindTimer.mean(TimeUnit.NANOSECONDS));
//            System.out.println(BPlusKVTable.recurFindTimer.totalTime(TimeUnit.SECONDS));

//            System.out.println("\ngetData");
//            System.out.println(BPlusKVTableUtils.getData.mean(TimeUnit.NANOSECONDS));
//            System.out.println(BPlusKVTableUtils.getData.totalTime(TimeUnit.SECONDS));
//
//            System.out.println("\ngetDataLocationFromLeaf");
//            System.out.println(BPlusKVTableUtils.getDataLocationFromLeaf.mean(TimeUnit.NANOSECONDS));
//            System.out.println(BPlusKVTableUtils.getDataLocationFromLeaf.totalTime(TimeUnit.SECONDS));
//
//            System.out.println("\nparseLeaf");
//            System.out.println(BPlusKVTableUtils.parseLeaf.mean(TimeUnit.NANOSECONDS));
//            System.out.println(BPlusKVTableUtils.parseLeaf.totalTime(TimeUnit.SECONDS));
//
//            System.out.println("\nfindLeafKey");
//            System.out.println(BPlusKVTableUtils.findLeafKey.mean(TimeUnit.NANOSECONDS));
//            System.out.println(BPlusKVTableUtils.findLeafKey.totalTime(TimeUnit.SECONDS));


//            System.out.println("\nTreeLeafNode.treeLeafGetInsertPos");
//            System.out.println(TreeLeafNode.treeLeafGetInsertPos.mean(TimeUnit.NANOSECONDS));
//            System.out.println(TreeLeafNode.treeLeafGetInsertPos.totalTime(TimeUnit.SECONDS));
//
//            System.out.println("\nTreeLeafNode.treeLeafGetKeyLoc");
//            System.out.println(TreeLeafNode.treeLeafGetKeyLoc.mean(TimeUnit.NANOSECONDS));
//            System.out.println(TreeLeafNode.treeLeafGetKeyLoc.totalTime(TimeUnit.SECONDS));
//            System.out.println(TreeLeafNode.treeLeafGetKeyLoc.count());
//
//            System.out.println("\nTreeLeafNode.treeLeafReadKey");
//            System.out.println(TreeLeafNode.treeLeafReadKey.mean(TimeUnit.NANOSECONDS));
//            System.out.println(TreeLeafNode.treeLeafReadKey.totalTime(TimeUnit.SECONDS));

//            System.out.println("\nTreeLeafNode.IndexPageManager.readPage");
//            System.out.println(IndexPageManager.readPage.mean(TimeUnit.NANOSECONDS));
//            System.out.println(IndexPageManager.readPage.totalTime(TimeUnit.SECONDS));


//            System.out.println("\nBufferNew.pullCacheOuter");
//            System.out.println(BufferNew.pullCacheOuter.mean(TimeUnit.NANOSECONDS));
//            System.out.println(BufferNew.pullCacheOuter.totalTime(TimeUnit.SECONDS));
//            System.out.println("\nBufferNew.pullCache");
//            System.out.println(BufferNew.pullCache.mean(TimeUnit.NANOSECONDS));
//            System.out.println(BufferNew.pullCache.totalTime(TimeUnit.SECONDS));
//            System.out.println("\nBufferNew.pullCache2");
//            System.out.println(BufferNew.pullCache2.mean(TimeUnit.NANOSECONDS));
//            System.out.println(BufferNew.pullCache2.totalTime(TimeUnit.SECONDS));
//            System.out.println("\nBufferNew.lockReadPage");
//            System.out.println(BufferNew.lockReadPage.mean(TimeUnit.NANOSECONDS));
//            System.out.println(BufferNew.lockReadPage.totalTime(TimeUnit.SECONDS));
//            System.out.println("\nBufferNew.updateCache");
//            System.out.println(BufferNew.updateCache.mean(TimeUnit.NANOSECONDS));
//            System.out.println(BufferNew.updateCache.totalTime(TimeUnit.SECONDS));
//            System.out.println("\nBufferNew.castPage");
//            System.out.println(BufferNew.castPage.mean(TimeUnit.NANOSECONDS));
//            System.out.println(BufferNew.castPage.totalTime(TimeUnit.SECONDS));


//            System.out.println("\nTreeLeafNode.IndexPageManager.parseToLocation");
//            System.out.println(IndexPageManager.parseToLocation.mean(TimeUnit.NANOSECONDS));
//            System.out.println(IndexPageManager.parseToLocation.totalTime(TimeUnit.SECONDS));
//
//            System.out.println("\nreadData");
//            System.out.println(BPlusKVTableUtils.readData.mean(TimeUnit.NANOSECONDS));
//            System.out.println(BPlusKVTableUtils.readData.totalTime(TimeUnit.SECONDS));
//            System.out.println(BPlusKVTableUtils.readData.count());

//            System.out.println("\nTreeKeyNode.treeKeyNodeGetInsertPos");
//            System.out.println(TreeKeyNode.treeKeyNodeGetInsertPos.mean(TimeUnit.NANOSECONDS));
//            System.out.println(TreeKeyNode.treeKeyNodeGetInsertPos.totalTime(TimeUnit.SECONDS));
//            System.out.println(TreeKeyNode.treeKeyNodeGetInsertPos.count());
//
//            System.out.println("\nTreeKeyNode.treeKeyNodeGetKeyLoc");
//            System.out.println(TreeKeyNode.treeKeyNodeGetKeyLoc.mean(TimeUnit.NANOSECONDS));
//            System.out.println(TreeKeyNode.treeKeyNodeGetKeyLoc.totalTime(TimeUnit.SECONDS));
//            System.out.println(TreeKeyNode.treeKeyNodeGetKeyLoc.count());
//
//            System.out.println("\nTreeKeyNode.treeKeyNodeReadKey");
//            System.out.println(TreeKeyNode.treeKeyNodeReadKey.mean(TimeUnit.NANOSECONDS));
//            System.out.println(TreeKeyNode.treeKeyNodeReadKey.totalTime(TimeUnit.SECONDS));
//            System.out.println(TreeKeyNode.treeKeyNodeReadKey.count());

//            System.out.println("func run");
//            System.out.println(timer3.mean(TimeUnit.NANOSECONDS));
//            System.out.println(timer3.totalTime(TimeUnit.SECONDS));


            System.out.println("KeyNode get: " + (TreeKeyNode.getCnt.get() - i1));
            System.out.println("KeyNode missed: " + (TreeKeyNode.missedCnt.get() - i2));
            System.out.println("LeafNode get: " + (TreeLeafNode.getCnt.get() - i3));
            System.out.println("LeafNode missed: " + (TreeLeafNode.missedCnt.get() - i4));
        }
    }

//    public static Timer timerShutDown = BPlusKVTable.registry.timer("timerShutDown");
//    public static Timer timerSubmit = BPlusKVTable.registry.timer("timerSubmit");
//    public static Timer timerAssert = BPlusKVTable.registry.timer("timerAssert");

    public static <EX extends Exception> void runConcurrentTest(int taskCount,
                                                                int threadPoolAwaitTimeoutSec,
                                                                int taskAwaitTimeoutSec,
                                                                UnCheckedConsumer<Integer, EX> function,
                                                                int numberOfThreads) throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(numberOfThreads);
        List<Future<?>> futures = new ArrayList<>(taskCount);
//        Time.withTimer(timerSubmit, () -> {

        for (int i = 0; i < taskCount; i++) {
            int y = i;
            Future<?> future1 = executor.submit(() -> {
                try {
                    function.accept(y);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
            futures.add(future1);
        }
//        });

//        Time.withTimer(timerShutDown, () -> {
        executor.shutdown();
        if (!executor.awaitTermination(threadPoolAwaitTimeoutSec, TimeUnit.SECONDS)) {
            throw new InterruptedException();
        }
//        });
//        Time.withTimer(timerAssert, () -> {
        assertEquals(taskCount, futures.size());
        for (Future<?> future : futures) {
            future.get(taskAwaitTimeoutSec, TimeUnit.SECONDS);
        }
//        });

    }

    public static void printThread(String msg) {
        System.out.println("Thread[" + Thread.currentThread().getId() + "]  Buffer: " + msg);
    }

}
