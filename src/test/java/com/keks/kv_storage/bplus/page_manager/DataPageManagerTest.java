package com.keks.kv_storage.bplus.page_manager;

import com.keks.kv_storage.record.KVRecord;
import com.keks.kv_storage.bplus.TableName;
import com.keks.kv_storage.bplus.buffer.PageBuffer;
import com.keks.kv_storage.bplus.conf.BPlusConf;
import com.keks.kv_storage.bplus.page_manager.managers.DataPageManager;
import com.keks.kv_storage.bplus.tree.KeyToDataLocationsItem;
import com.keks.kv_storage.bplus.tree.node.leaf.LeafDataLocation;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;


class DataPageManagerTest {

    private static final String value700Len = "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Phasellus rhoncus elit quis " +
            "augue mollis, eget interdum augue ornare. Sed sit amet orci at arcu pulvinar tincidunt. Cras in sem " +
            "nibh. Mauris facilisis varius congue. Curabitur placerat ut nibh dictum ultricies. Class aptent " +
            "taciti sociosqu ad litora torquent per conubia nostra, per inceptos himenaeos. Aliquam semper erat " +
            "sed ex finibus, sodales tempus massa sodales. Sed dictum mattis tellus non mollis. Curabitur " +
            "imperdiet feugiat risus nec tempus. Nullam porta nulla quis ligula aliquet faucibus. Praesent " +
            "convallis eleifend est, eu facilisis ante. Sed cursus commodo sapien, vitae condimentum lacus " +
            "convallis vel. Curabitur ut nibh varius";
    private static final TableName tableName = new TableName("test");

    @Test
    public void test1(@TempDir Path dir) throws IOException {
        PageBuffer myBuffer = new PageBuffer(10);
//        TablePagesManager pagesManager = new TablePagesManager(
//                tableName,
//                new TablePagesManager.PagesManagerConf(dir.toFile(), 1, 1),
//                myBuffer);
//        TableDataPageManager dataPageManager = new TableDataPageManager(tableName, dir.toFile(), pagesManager, myBuffer, 1);

        File tableDir = new File(dir.toFile(), tableName.name);
        tableDir.mkdir();
        DataPageManager dataPageManager = new DataPageManager(tableDir, myBuffer, new BPlusConf(3, 10, 10,400_000_00));

        HashMap<Integer, LeafDataLocation[]> map = new HashMap<>();
        int records = 100;
        for (int i = 0; i < records; i++) {
            String key = "key" + i;
            {
                String value = value700Len.repeat(6);
                KVRecord keyDataTupleToSplit = new KVRecord(key, value.getBytes());
                LeafDataLocation[] locations = dataPageManager.addNewKeyDataTuple(keyDataTupleToSplit);
                map.put(i, locations);
            }
            {
                if (i % 2 == 0) {
                    String value = value700Len.repeat(12);
                    LeafDataLocation[] previousLocations = map.get(i);
                    KVRecord keyDataTupleToSplit = new KVRecord(key, value.getBytes());
                    LeafDataLocation[] locations = dataPageManager.replaceData(previousLocations, keyDataTupleToSplit);
                    map.put(i, locations);
                } else if (i % 3 == 0) {
                    String value = value700Len;
                    LeafDataLocation[] previousLocations = map.get(i);
                    KVRecord keyDataTupleToSplit = new KVRecord(key, value.getBytes());
                    LeafDataLocation[] locations = dataPageManager.replaceData(previousLocations, keyDataTupleToSplit);
                    map.put(i, locations);
                }
            }
        }

        for (int i = 0; i < records; i++) {
            String key = "key" + i;
            String value;
            if (i % 2 == 0) {
                value = value700Len.repeat(12);
            } else if (i % 3 == 0) {
                value = value700Len;
            } else {
                value = value700Len.repeat(6);
            }
            KVRecord exp = new KVRecord(key, value.getBytes());
            KVRecord keyDataTuple = dataPageManager.getKeyDataTuple(new KeyToDataLocationsItem("", exp.getLen(), map.get(i)));
            assertEquals(exp.key, keyDataTuple.key);
            assertEquals(value, new String(keyDataTuple.valueBytes));

        }

    }

    // same as test1() but concurrently
    @Test
    public void test2(@TempDir Path dir) throws IOException, ExecutionException, InterruptedException, TimeoutException {
        PageBuffer myBuffer = new PageBuffer(2_000_000);

        File tableDir = new File(dir.toFile(), tableName.name);
        tableDir.mkdir();
        DataPageManager dataPageManager = new DataPageManager(tableDir, myBuffer, new BPlusConf(3, 10, 10,400_000_00));


        ConcurrentSkipListMap<Integer, LeafDataLocation[]> map = new ConcurrentSkipListMap<>();
        final AtomicInteger cnt = new AtomicInteger(0);
        int records = 100_000;
        {
            Function<Integer, String> func = i -> {
                if (cnt.incrementAndGet() % 10_000 == 0)
                    System.out.println(cnt.get());
                String key = "key" + i;
                try {
                    {
                        String value = value700Len.repeat(6);
                        KVRecord keyDataTupleToSplit = new KVRecord(key, value.getBytes());
                        LeafDataLocation[] locations = dataPageManager.addNewKeyDataTuple(keyDataTupleToSplit);
                        map.put(i, locations);
                    }
                    {
                        if (i % 2 == 0) {
                            String value = value700Len.repeat(12);
                            LeafDataLocation[] previousLocations = map.get(i);
                            KVRecord keyDataTupleToSplit = new KVRecord(key, value.getBytes());
                            LeafDataLocation[] locations = dataPageManager.replaceData(previousLocations, keyDataTupleToSplit);
                            map.put(i, locations);
                        }
                        else
                        if (i % 3 == 0) {
                            String value = value700Len;
                            LeafDataLocation[] previousLocations = map.get(i);
                            KVRecord keyDataTupleToSplit = new KVRecord(key, value.getBytes());
                            LeafDataLocation[] locations = dataPageManager.replaceData(previousLocations, keyDataTupleToSplit);
                            map.put(i, locations);
                        }
//
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
                return "";
            };
            runConcurrentTest(records, 1000, 1, func, 200);
        }

        System.out.println("Asserting");
        {
            Function<Integer, String> func = i -> {
                String key = "key" + i;
                try {
                    String value;
                    if (i % 2 == 0) {
                        value = value700Len.repeat(12);
                    } else if (i % 3 == 0) {
                        value = value700Len;
                    } else {
                        value = value700Len.repeat(6);
                    }
                    KVRecord exp = new KVRecord(key, value.getBytes());
                    KVRecord keyDataTuple = dataPageManager.getKeyDataTuple(new KeyToDataLocationsItem("", exp.getLen(), map.get(i)));
                    assertEquals(exp.key, keyDataTuple.key);
                    assertEquals(value, new String(keyDataTuple.valueBytes));
                } catch (IOException e) {
                    e.printStackTrace();
                } finally {
                    return "";
                }
            };
            runConcurrentTest(records, 1000, 1, func, 200);
        }

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