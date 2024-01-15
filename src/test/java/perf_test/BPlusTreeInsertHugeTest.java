package perf_test;


import com.keks.kv_storage.bplus.TableName;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;


public class BPlusTreeInsertHugeTest {

    private static final TableName tableName = new TableName("test");

    // insert and update
    // 51sec
//    @Test
//    public void test3(@TempDir Path dir) throws Exception {
//        BufferNew buffer = new BufferNew(50_000);
//        int numberOfThreads = 200;
//        BPlusEngineConf bPlusEngineConf = new BPlusEngineConf(400, 1022, 1022);
//        BPlusKVTable kvTable = BPlusKVTable.createNewTable(dir.toFile(), bPlusEngineConf, buffer);
////PT48.290377S, 1
//        // PT11.797962S, 2
//        //PT12.780693S, 5
//        //PT13.216099S, 10
//        //PT12.401958S, 50
////        int records = 1_000_000; // PT1M0.367644S
////        int records = 300_000;
//        int taskCount = 1_000_000;
//
//        AtomicInteger cnt = new AtomicInteger(0);
//        Function<Integer, String> func = i -> {
//            String key = "key" + i;
//            String value = "value" + i;
//            try {
//
//                kvTable.addWithWriteLock(new KVRecord(key, value.getBytes()));
////
//                assertEquals(value, new String(kvTable.get(key).valueBytes));
//
//                if (i % 2 == 0) {
//                    value = "value";
//                    kvTable.addWithWriteLock(new KVRecord(key, value.getBytes()));
//                    assertEquals(value, new String(kvTable.get(key).valueBytes));
//                }
//
//                if (i % 3 == 0) {
//                   kvTable.deleteWithWriteLock(key);
//
//                }
//
//                return "";
//            } catch (IOException e) {
//                e.printStackTrace();
//                throw new RuntimeException();
//            } catch (Throwable t) {
//                System.out.println("Failed on key: " + key);
//                t.printStackTrace();
//                throw new RuntimeException(t);
//            } finally {
//                int ccc = cnt.incrementAndGet();
//                if (ccc % 10_000 == 0) System.err.println(ccc);
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
//    @Test
//    public void test4(@TempDir Path dir) throws IOException, ExecutionException, InterruptedException, TimeoutException {
////        BufferNew myBuffer = new BufferNew(1_000_000);
//        BtreeParameters btreeParameters = new BtreeParameters(BtreeParameters.MAX_ORDER);
////        BPlusKVTable kvTable = BPlusKVTable.createNewTable(dir.toFile(), btreeParameters, myBuffer);
//        BPlusKVTable kvTable = BPlusKVTable.createNew(new TableName("test-kv"), dir.toFile(), btreeParameters, 50_000);
//
////PT48.290377S, 1
//        // PT11.797962S, 2
//        //PT12.780693S, 5
//        //PT13.216099S, 10
//        //PT12.401958S, 50
//        int records = 1_000_000; //
////        int records = 500;
//
//        ArrayList<String> list = new ArrayList<>();
//        for (int i = 0; i < records; i++) {
//            list.add("key" + String.format("%07d", i));
//        }
//        Collections.shuffle(list);
//
//        System.out.println("Running");
//        AtomicInteger cnt = new AtomicInteger(0);
//        Function<Integer, String> func = i -> {
//            String key = "";
//            try {
//                key = list.get(i);
//                String value = "value" + String.format("%07d", i);
//
//                kvTable.put(key, value.getBytes());
//
//                assertEquals(value, new String(kvTable.get(key)));
//
//                if (i % 2 == 0) {
//                    value = "value";
//                    kvTable.put(key, value.getBytes());
//                    assertEquals(value, new String(kvTable.get(key)));
//
//                }
//
//                if (i % 3 == 0) {
//                    kvTable.remove(key);
//                }
//
//                return "";
//            } catch (IOException e) {
//                e.printStackTrace();
//                throw new RuntimeException();
//            } catch (Throwable t) {
//                System.out.println("Failed on key: " + key);
//                t.printStackTrace();
//                throw new RuntimeException(t);
//            } finally {
//                int ccc = cnt.incrementAndGet();
//                if (ccc % 10_000 == 0) System.err.println(ccc);
//            }
//
//        };
//
//        Instant start = Instant.now();
//        runConcurrentTest(records, 20000, 1, func, 50);
//        Instant finish = Instant.now();
//        Duration between = Duration.between(start, finish);
//        System.out.println(between);
//
////        System.out.println("Tree height: " + kvTable.treeNodePageManager.treeHeader.getTreeHeight());
////        System.out.println("Cnt Read Add: " + kvTable.cntReadAdd.get());
////        System.out.println("Cnt Write Add: " + kvTable.cntWriteAdd.get());
////
////        System.out.println("TreePageRead: " + myBuffer.treePageCnt.get());
////        System.out.println("IndexPageRead: " + myBuffer.indexPageCnt.get());
////        System.out.println("DataPageRead: " + myBuffer.dataPageCnt.get());
//
//
//        // readLock
//        //Avg: 1931905.0108932462
//        //Avg: PT0.001931905S
//        //Size: 397035
//        // writeLock
//        //Avg: 56124.876420074135
//        //Avg: PT0.000056124S
//        //Size: 399539
//
//
////        while (true) {
////
////        }
////        System.out.println(myBuffer.getBuffStat());
////        System.out.println("Success try: " + myBuffer.trySuccessCnt);
////        System.out.println("Success but null try: " + myBuffer.trySuccessButNullCnt);
////        System.out.println("Fail try: " + myBuffer.tryFailCnt);
////        myBuffer.flushAll();
////        for (PageStatistics pagesStatistic : kvTable.dataPageManager.getPagesStatistics()) {
////            System.out.println(pagesStatistic);
////        }
//    }

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
