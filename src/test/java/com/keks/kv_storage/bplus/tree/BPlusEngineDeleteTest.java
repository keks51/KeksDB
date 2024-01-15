package com.keks.kv_storage.bplus.tree;

import com.keks.kv_storage.bplus.BPlusEngine;
import com.keks.kv_storage.bplus.buffer.PageBuffer;
import com.keks.kv_storage.bplus.conf.BPlusConf;
import com.keks.kv_storage.record.KVRecord;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import utils.TestUtils;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.*;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;


class BPlusEngineDeleteTest {

    //delete always left (root and leaf)
    @Test
    public void test5(@TempDir Path dir) throws IOException {

        {
            BPlusConf btreeParams = new BPlusConf(4, 10, 10,400_000);
            BPlusEngine kvTable = BPlusEngine.createNewTable("", dir.toFile(), btreeParams);

            ArrayList<KVRecord> expList = new ArrayList<>();
            int maxValue = 8;
            for (int i = 0; i <= maxValue; i++) {
                String key = "key" + String.format("%03d", i);
                String value = "value" + i;

                kvTable.addWithWriteLock(new KVRecord(key, value.getBytes()));
                expList.add(new KVRecord(key, value.getBytes()));
            }
            kvTable.printFreeSpaceInfo();
            expList.sort(Comparator.comparing(o -> o.key));

            

            ArrayList<KVRecord> all = kvTable.getAll();
            assertEquals(expList, all);


            System.out.println();
            kvTable.deleteWithWriteLock("key000");
            
            expList.remove(0);
            all.remove(0);
            assertEquals(expList, all);


            System.out.println();
            kvTable.deleteWithWriteLock("key001");
            
            expList.remove(0);
            all.remove(0);
            assertEquals(expList, all);

            System.out.println();
            kvTable.deleteWithWriteLock("key002");
            
            expList.remove(0);
            all.remove(0);
            assertEquals(expList, all);

            System.out.println();
            kvTable.deleteWithWriteLock("key003");
            
            expList.remove(0);
            all.remove(0);
            assertEquals(expList, all);

            System.out.println();
            kvTable.deleteWithWriteLock("key004");
            
            expList.remove(0);
            all.remove(0);
            assertEquals(expList, all);

            System.out.println();
            kvTable.deleteWithWriteLock("key005");
            
            expList.remove(0);
            all.remove(0);
            assertEquals(expList, all);

            System.out.println();
            kvTable.deleteWithWriteLock("key006");
            
            expList.remove(0);
            all.remove(0);
            assertEquals(expList, all);

            System.out.println();
            kvTable.deleteWithWriteLock("key007");
            
            expList.remove(0);
            all.remove(0);
            assertEquals(expList, all);

            System.out.println();
            kvTable.deleteWithWriteLock("key008");
            
            expList.remove(0);
            all.remove(0);
            assertEquals(expList, all);

            kvTable.printFreeSpaceInfo();
        }


    }


    //delete always right (root and leaf)
    @Test
    public void test6(@TempDir Path dir) throws IOException {
        BPlusConf btreeParams = new BPlusConf(4, 10, 10,400_000);
        BPlusEngine kvTable = BPlusEngine.createNewTable("", dir.toFile(), btreeParams);

        ArrayList<KVRecord> expList = new ArrayList<>();
        int maxValue = 8;
        for (int i = 0; i <= maxValue; i++) {
            String key = "key" + String.format("%03d", i);
            String value = "value" + i;

            kvTable.addWithWriteLock(new KVRecord(key, value.getBytes()));
            expList.add(new KVRecord(key, value.getBytes()));
        }
        expList.sort(Comparator.comparing(o -> o.key));

        

        ArrayList<KVRecord> all = kvTable.getAll();
        assertEquals(expList, all);


        kvTable.deleteWithWriteLock("key008");
        
        expList.remove(expList.size() - 1);
        all.remove(all.size() - 1);
        assertEquals(expList, all);


        kvTable.deleteWithWriteLock("key007");
        
        expList.remove(expList.size() - 1);
        all.remove(all.size() - 1);
        assertEquals(expList, all);

        kvTable.deleteWithWriteLock("key006");
        
        expList.remove(expList.size() - 1);
        all.remove(all.size() - 1);
        assertEquals(expList, all);

        kvTable.deleteWithWriteLock("key005");
        
        expList.remove(expList.size() - 1);
        all.remove(all.size() - 1);
        assertEquals(expList, all);

        kvTable.deleteWithWriteLock("key004");
        
        expList.remove(expList.size() - 1);
        all.remove(all.size() - 1);
        assertEquals(expList, all);

        kvTable.deleteWithWriteLock("key003");
        
        expList.remove(expList.size() - 1);
        all.remove(all.size() - 1);
        assertEquals(expList, all);

        kvTable.deleteWithWriteLock("key001");
        
        expList.remove(expList.size() - 1);
        all.remove(all.size() - 1);
        assertEquals(expList, all);


        kvTable.deleteWithWriteLock("key000");
        
        expList.remove(expList.size() - 1);
        all.remove(all.size() - 1);
        assertEquals(expList, all);

        kvTable.deleteWithWriteLock("key002");
        
        expList.remove(expList.size() - 1);
        all.remove(all.size() - 1);
        assertEquals(expList, all);


    }

    //delete random shuffle (root and leaf)
    @RepeatedTest(10)
    public void test7(@TempDir Path dir) throws IOException {

        {
            BPlusConf btreeParams = new BPlusConf(4, 10, 10,4_000_000);
            BPlusEngine kvTable = BPlusEngine.createNewTable("", dir.toFile(), btreeParams);

            ArrayList<KVRecord> expList = new ArrayList<>();
            ArrayList<KVRecord> dataList = new ArrayList<>();
            int maxValue = 100;
            for (int i = 0; i <= maxValue; i++) {
                String key = "key" + String.format("%03d", i);
                String value = "value" + i;
//                kvTable.add(key, value.getBytes());
                dataList.add(new KVRecord(key, value.getBytes()));
                expList.add(new KVRecord(key, value.getBytes()));
            }
            Collections.shuffle(dataList);

            System.out.println("Adding");
            for (KVRecord keyDataTuple : dataList) {
                System.out.println(keyDataTuple.key);
                kvTable.addWithWriteLock(keyDataTuple);
            }
            
            Collections.shuffle(dataList);

            for (int i = 0; i <= 8; i++) {
                KVRecord toRemove = dataList.remove(0);
                kvTable.deleteWithWriteLock(toRemove.key);
                
                expList.remove(toRemove);
                ArrayList<KVRecord> all = kvTable.getAll();
                assertEquals(expList, all);
            }
            
        }


    }

    //delete random (root and leaf)
    @Test
    public void test8(@TempDir Path dir) throws IOException {

        {
            BPlusConf btreeParams = new BPlusConf(4, 10, 10,400_000);
            BPlusEngine kvTable = BPlusEngine.createNewTable("", dir.toFile(), btreeParams);

            ArrayList<KVRecord> expList = new ArrayList<>() {{
                add(new KVRecord("key004", "value_key004".getBytes()));
                add(new KVRecord("key008", "value_key008".getBytes()));
                add(new KVRecord("key002", "value_key002".getBytes()));
                add(new KVRecord("key001", "value_key001".getBytes()));
                add(new KVRecord("key005", "value_key005".getBytes()));
                add(new KVRecord("key000", "value_key000".getBytes()));
                add(new KVRecord("key006", "value_key006".getBytes()));
                add(new KVRecord("key007", "value_key007".getBytes()));
                add(new KVRecord("key003", "value_key003".getBytes()));
            }};

            for (KVRecord e : expList) {
                kvTable.addWithWriteLock(e);
            }

            expList.sort(Comparator.comparing(o -> o.key));

            

            ArrayList<KVRecord> all = kvTable.getAll();
            assertEquals(expList, all);


            System.out.println();
            System.out.println("Remove key: key001");
            kvTable.deleteWithWriteLock("key001");
            
            expList.remove(expList.size() - 1);
            all.remove(all.size() - 1);
            assertEquals(expList, all);

            System.out.println();
            System.out.println("Remove key: key003");
            kvTable.deleteWithWriteLock("key003");
            
            expList.remove(expList.size() - 1);
            all.remove(all.size() - 1);
            assertEquals(expList, all);

            System.out.println();
            System.out.println("Remove key: key007");
            kvTable.deleteWithWriteLock("key007");
            
            expList.remove(expList.size() - 1);
            all.remove(all.size() - 1);
            assertEquals(expList, all);

            System.out.println();
            System.out.println("Remove key: key004");
            kvTable.deleteWithWriteLock("key004");
            
            expList.remove(expList.size() - 1);
            all.remove(all.size() - 1);
            assertEquals(expList, all);

            System.out.println();
            System.out.println("Remove key: key000");
            kvTable.deleteWithWriteLock("key000");
            
            expList.remove(expList.size() - 1);
            all.remove(all.size() - 1);
            assertEquals(expList, all);

            System.out.println();
            System.out.println("Remove key: key006");
            kvTable.deleteWithWriteLock("key006");
            
            expList.remove(expList.size() - 1);
            all.remove(all.size() - 1);
            assertEquals(expList, all);

            System.out.println();
            System.out.println("Remove key: key008");
            kvTable.deleteWithWriteLock("key008");
            
            expList.remove(expList.size() - 1);
            all.remove(all.size() - 1);
            assertEquals(expList, all);
        }


    }

    //delete random (root and leaf)
    @Test
    public void test81(@TempDir Path dir) throws IOException {

        {
            BPlusConf btreeParams = new BPlusConf(5, 10, 10,400_000);
            BPlusEngine kvTable = BPlusEngine.createNewTable("", dir.toFile(), btreeParams);

            ArrayList<KVRecord> expList = new ArrayList<>() {{
                add(new KVRecord("key0008", "value_key0008".getBytes()));
                add(new KVRecord("key0003", "value_key0003".getBytes()));
                add(new KVRecord("key0000", "value_key0000".getBytes()));
                add(new KVRecord("key0005", "value_key0005".getBytes()));
                add(new KVRecord("key0001", "value_key0001".getBytes()));
                add(new KVRecord("key0009", "value_key0009".getBytes()));
                add(new KVRecord("key0002", "value_key0002".getBytes()));
                add(new KVRecord("key0006", "value_key0006".getBytes()));
                add(new KVRecord("key0007", "value_key0007".getBytes()));
                add(new KVRecord("key0004", "value_key0004".getBytes()));
            }};

            for (KVRecord e : expList) {
                kvTable.addWithWriteLock(e);
            }

            expList.sort(Comparator.comparing(o -> o.key));

            

            ArrayList<KVRecord> all = kvTable.getAll();
            assertEquals(expList, all);


            System.out.println();
            System.out.println("Remove key: key0001");
            kvTable.deleteWithWriteLock("key0001");
            
            expList.removeIf(e -> e.key.equals("key0001"));
            all = kvTable.getAll();
            assertEquals(expList, all);

            System.out.println();
            System.out.println("Remove key: key0005");
            kvTable.deleteWithWriteLock("key0005");
            
            expList.removeIf(e -> e.key.equals("key0005"));
            all = kvTable.getAll();
            assertEquals(expList, all);

            System.out.println();
            System.out.println("Remove key: key0002");
            kvTable.deleteWithWriteLock("key0002");
            
            expList.removeIf(e -> e.key.equals("key0002"));
            all = kvTable.getAll();
            assertEquals(expList, all);

            System.out.println();
            System.out.println("Remove key: key0004");
            kvTable.deleteWithWriteLock("key0004");
            
            expList.removeIf(e -> e.key.equals("key0004"));
            all = kvTable.getAll();
            assertEquals(expList, all);

            System.out.println();
            System.out.println("Remove key: key0000");
            kvTable.deleteWithWriteLock("key0000");
            
            expList.removeIf(e -> e.key.equals("key0000"));
            all = kvTable.getAll();
            assertEquals(expList, all);

            System.out.println();
            System.out.println("Remove key: key0006");
            kvTable.deleteWithWriteLock("key0006");
            
            expList.removeIf(e -> e.key.equals("key0006"));
            all = kvTable.getAll();
            assertEquals(expList, all);

            System.out.println();
            System.out.println("Remove key: key0008");
            kvTable.deleteWithWriteLock("key0008");
            
            expList.removeIf(e -> e.key.equals("key0008"));
            all = kvTable.getAll();
            assertEquals(expList, all);


        }


    }

    //delete always left (multiply parents)
    @Test
    public void test9(@TempDir Path dir) throws IOException {
        BPlusConf btreeParams = new BPlusConf(4, 10, 10,400_000_000);
        BPlusEngine kvTable = BPlusEngine.createNewTable("", dir.toFile(), btreeParams);
//        kvTable.printFreeSpaceInfo();
        int maxValue = 1000;
        {
            ArrayList<KVRecord> expList = new ArrayList<>();
            for (int i = 0; i <= maxValue; i++) {
                String key = "key" + String.format("%04d", i);
                String value = "value" + i;

                kvTable.addWithWriteLock(new KVRecord(key, value.getBytes()));
                expList.add(new KVRecord(key, value.getBytes()));
//                if (i==3) {
//                    kvTable.printTree();
//                    System.out.println();
//                }
//                if (i==5) {
//                    kvTable.printTree();
//                    System.out.println();
//                }
//                if (i==7) {
//                    kvTable.printTree();
//                    System.out.println();
//                }
//                if (i==14) {
//                    kvTable.printTree();
//                    System.out.println();
//                }
//                if (i==15) {
//                    kvTable.printTree();
//                    System.out.println();
//                }
            }
            expList.sort(Comparator.comparing(o -> o.key));
//            myBuffer.flushAll();
//            System.out.println("here123");
//            System.out.println(kvTable.indexPageManager.printPagesInfo());
//            kvTable.printFreeSpaceInfo();
//            System.out.println();
//            myBuffer.flushAll();
//            System.out.println(kvTable.indexPageManager.printPagesInfo());
//            System.out.println(kvTable.dataPageManager.printPagesInfo());

            ArrayList<KVRecord> all = kvTable.getAll();
//            List<String> list = IntStream
//                    .range(0, Math.min(all.size(), expList.size()))
//                    .mapToObj(i -> all.get(i) + ":" + expList.get(i))
//                    .collect(Collectors.toList());
//            list.forEach(System.out::println);

            assertEquals(expList, all);
//            kvTable.printTree();
            System.out.println("deleting");

            for (int i = 0; i <= maxValue; i++) {
                String key = "key" + String.format("%04d", i);
//                System.out.println(key);
                assertEquals("value" + i, new String(kvTable.get(key).valueBytes));
                kvTable.deleteWithWriteLock(key);
                expList.remove(0);
                all.remove(0);
                assertEquals(expList, all);
//                myBuffer.flushAll();
//                kvTable.printFreeBitsSetTreeNodeManager();
            }
//            kvTable.printFreeSpaceInfo();
//            myBuffer.flushAll();
//            System.out.println(kvTable.indexPageManager.printPagesInfo());
//            System.out.println(kvTable.dataPageManager.printPagesInfo());
        }
//        System.out.println(kvTable.indexPageManager.printPagesInfo());

        {
            ArrayList<KVRecord> expList = new ArrayList<>();

            for (int i = 0; i <= maxValue; i++) {
                String key = "key" + String.format("%04d", i);
                String value = "value" + i;

                kvTable.addWithWriteLock(new KVRecord(key, value.getBytes()));
                expList.add(new KVRecord(key, value.getBytes()));
            }
            expList.sort(Comparator.comparing(o -> o.key));

            ArrayList<KVRecord> all = kvTable.getAll();
            assertEquals(expList, all);

            for (int i = 0; i <= maxValue; i++) {
                String key = "key" + String.format("%04d", i);
                assertEquals("value" + i, new String(kvTable.get(key).valueBytes));
//                System.out.println("Delete: " + key);
//                if (key.equals("key019"))
//                    System.out.println();
                kvTable.deleteWithWriteLock(key);
                expList.remove(0);
                all.remove(0);
                assertEquals(expList, all);
            }
        }


    }

    //delete always right (multiply parents)
    @Test
    public void test10(@TempDir Path dir) throws IOException {
        BPlusConf btreeParams = new BPlusConf(3, 10, 10,40_000_000);
        BPlusEngine kvTable = BPlusEngine.createNewTable("", dir.toFile(), btreeParams);
        int maxValue = 100;
        {
            ArrayList<KVRecord> expList = new ArrayList<>();

            for (int i = 0; i <= maxValue; i++) {
                String key = "key" + String.format("%03d", i);
                String value = "value" + i;

                kvTable.addWithWriteLock(new KVRecord(key, value.getBytes()));
                expList.add(new KVRecord(key, value.getBytes()));
            }
            expList.sort(Comparator.comparing(o -> o.key));

            ArrayList<KVRecord> all = kvTable.getAll();
            assertEquals(expList, all);

            for (int i = 0; i <= maxValue; i++) {
                String key = "key" + String.format("%03d", maxValue - i);
                assertEquals("value" + (maxValue - i), new String(kvTable.get(key).valueBytes));

                kvTable.deleteWithWriteLock(key);
                expList.remove(expList.size() - 1);
                all.remove(all.size() - 1);
                assertEquals(expList, all);
            }
        }

        System.out.println("next run");
        {
            ArrayList<KVRecord> expList = new ArrayList<>();

            for (int i = 0; i <= maxValue; i++) {
                String key = "key" + String.format("%03d", i);
                String value = "value" + i;
                System.out.println("Add: " + key);
                kvTable.addWithWriteLock(new KVRecord(key, value.getBytes()));
                expList.add(new KVRecord(key, value.getBytes()));
            }
            expList.sort(Comparator.comparing(o -> o.key));
            ArrayList<KVRecord> all = kvTable.getAll();
            assertEquals(expList, all);

            for (int i = 0; i <= maxValue; i++) {
                String key = "key" + String.format("%03d", maxValue - i);
                assertEquals("value" + (maxValue - i), new String(kvTable.get(key).valueBytes));
                kvTable.deleteWithWriteLock(key);
                expList.remove(expList.size() - 1);
                all.remove(all.size() - 1);
                assertEquals(expList, all);
            }
        }


    }

    //delete random shuffle (root and leaf)
    @RepeatedTest(10)
    public void test11(@TempDir Path dir) throws IOException {

        int[] orders = new int[]{3, 4, 5, 6, 10, 31, 100, 201};
//        int[] orders = new int[]{6};
        for (int order : orders) {
            TestUtils.emptyDir(dir.toFile());
            BPlusConf btreeParams = new BPlusConf(order, 10, 10,400_000_000);
            BPlusEngine kvTable = BPlusEngine.createNewTable("", dir.toFile(), btreeParams);

            ArrayList<KVRecord> expList = new ArrayList<>();
            ArrayList<KVRecord> dataList = new ArrayList<>();
            int maxValue = 1000;
            for (int i = 0; i <= maxValue; i++) {
                String key = "key" + String.format("%04d", i);
                String value = "value" + key;
                dataList.add(new KVRecord(key, value.getBytes()));
                expList.add(new KVRecord(key, value.getBytes()));
            }
            Collections.shuffle(dataList);
//            for (KVRecord kvRecord : dataList) {
//                System.out.print(kvRecord.key + ":" + new String(kvRecord.valueBytes) + "_");
//            }
//            System.out.println();

            for (KVRecord keyDataTuple : dataList) {
                kvTable.addWithWriteLock(keyDataTuple);
            }
            Collections.shuffle(dataList);
//            for (KVRecord kvRecord : dataList) {
//                System.out.print(kvRecord.key + ":" + new String(kvRecord.valueBytes) + "_");
//            }
//            System.out.println();

            for (int i = 0; i <= maxValue; i++) {
                KVRecord toRemove = dataList.remove(0);
                KVRecord kvRecord = kvTable.get(toRemove.key);
                if (kvRecord == null) {
                    ArrayList<KVRecord> all = kvTable.getAll();
                    System.out.println("");
                }
                assertEquals("value" + toRemove.key, new String(kvRecord.valueBytes)); // failed here
                kvTable.deleteWithWriteLock(toRemove.key);
                expList.remove(toRemove);
                ArrayList<KVRecord> all = kvTable.getAll();
                assertEquals(expList, all);
            }
        }


    }


    //delete concurrent
    @ParameterizedTest
    @ValueSource(ints = {3, 5, 10, 31, 100, 201})
    public void test12(int treeOrder, @TempDir Path dir) throws IOException, ExecutionException, InterruptedException, TimeoutException {
        BPlusConf btreeParams = new BPlusConf(treeOrder, 10, 10,40_000_000);
        BPlusEngine kvTable = BPlusEngine.createNewTable("", dir.toFile(), btreeParams);

        int records = 1_000;

        for (int i = 0; i < records; i++) {
            String key = "key" + String.format("%04d", i);
            String value = "value" + key;

            kvTable.addWithWriteLock(new KVRecord(key, value.getBytes()));
        }

        Function<Integer, String> func = i -> {
            String key = "key" + String.format("%04d", i);
            try {
                kvTable.deleteWithWriteLock(key);
                return "";
            } catch (IOException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }

        };

        runConcurrentTest(records, 1000, 10, func, 200);

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