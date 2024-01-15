package perf_test.concurrent;

import com.keks.kv_storage.record.KVRecord;
import com.keks.kv_storage.lsm.conf.LsmConf;
import com.keks.kv_storage.lsm.LsmEngine;
import com.keks.kv_storage.query.Query;
import com.keks.kv_storage.query.QueryIterator;
import com.keks.kv_storage.utils.SimpleScheduler;
import com.keks.kv_storage.utils.Time;
import com.keks.kv_storage.utils.UnCheckedConsumer;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.cumulative.CumulativeTimer;
import io.micrometer.core.instrument.distribution.HistogramSnapshot;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import utils.TestUtils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;


class LsmConcurrentTest {

    SimpleScheduler scheduler = new SimpleScheduler();

    @Test
    public void testKVTable0(@TempDir Path tmpPath) throws Exception {
        {
            int taskCount = 10_000;
//        int taskCount = 2_000;

            String kvTableName = "test1";
            LsmConf lsmConf = new LsmConf(128, 1_000, 0.99);
            File tableDir = tmpPath.resolve(kvTableName).toFile();
            tableDir.mkdir();
            LsmEngine lsmEngine = LsmEngine.createNewTable("", tableDir, lsmConf, scheduler);

            for (int i = 0; i < taskCount; i++) {
                String key = "key" + i;
                String value = "value" + i;

                lsmEngine.put(new KVRecord(key, value));
            }

            lsmEngine.forceFlush();

            System.out.println("fdfdfdfdf");
            Time.withTime("get", () -> {
                        for (int i = 0; i < taskCount; i++) {
                            if (i % 100_000 == 0) System.out.println(i);
                            String key = "key" + i;
                            String value = "value" + i;
                            assertEquals(value, new String(lsmEngine.get(key).valueBytes));
                        }
                    }

            );
            //Adding : PT29.587786S
            lsmEngine.close();
        }

        {
            int taskCount = 10_000;
//        int taskCount = 2_000;

            String kvTableName = "test1";
            LsmConf lsmConf = new LsmConf(128, 1_000, 0.99);
            File tableDir = tmpPath.resolve(kvTableName).toFile();
            tableDir.mkdir();
            LsmEngine lsmEngine = LsmEngine.loadTable("", tableDir, scheduler);

//            for (int i = 0; i < taskCount; i++) {
//                String key = "key" + i;
//                String value = "value" + i;
//
//                lsmEngine.put(key, value);
//            }

//            lsmEngine.flushAllForce();

            System.out.println("fdfdfdfdf");
            Time.withTime("get", () -> {
                        for (int i = 0; i < taskCount; i++) {
                            if (i % 100_000 == 0) System.out.println(i);
                            String key = "key" + i;
                            String value = "value" + i;
                            assertEquals(value, new String(lsmEngine.get(key).valueBytes));
                        }
                    }

            );
            //Adding : PT29.587786S
        }
    }

    @Test
    public void testKVTable1(@TempDir Path tmpPath) throws Exception {
        int taskCount = 2_000_000;
        int numberOfThreads = 200;
        String kvTableName = "test1";
        LsmConf lsmConf = new LsmConf(128, 100_000, 0.99);
        File tableDir = tmpPath.resolve(kvTableName).toFile();
        tableDir.mkdir();
        LsmEngine lsmEngine = LsmEngine.createNewTable("", tableDir, lsmConf, scheduler);

        Function<Integer, String> func1 = i -> {
            try {
                String key = "key" + i;
                String value = "value" + i;

                lsmEngine.put(new KVRecord(key, value));
//                assertEquals(value, new String(lsmKvTable.get(key).valueBytes));
//                if (i % 3 == 0) {
//                    lsmKvTable.remove(key);
//                    assertNull(lsmKvTable.get(key));
//                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            return "";
        };

        Time.withTime("Adding", () ->
                TestUtils.runConcurrentTest(taskCount, 10000, 1, func1, numberOfThreads)
        );
    }
    //put : PT22.117076S
    //get : PT14M47.463477S
    //get optimized : PT2M24.247063S
//    @RepeatedTest(100)
    @Test
    public void testPutAndGet(@TempDir Path tmpPath) throws Exception {
        try {
            System.out.println(tmpPath.toUri());
//        int taskCount = 10_000_000;
            int taskCount = 500_000;
            int numberOfThreads = 200;
            String kvTableName = "test1";
            LsmConf lsmConf = new LsmConf(400, 100_000, 0.5, false, 1000);
            File tableDir = tmpPath.resolve(kvTableName).toFile();
            tableDir.mkdir();
            LsmEngine lsmEngine = LsmEngine.createNewTable("", tableDir, lsmConf, scheduler);

            {
                AtomicInteger cntPut = new AtomicInteger(0);
                UnCheckedConsumer<Integer, IOException> func1 = i -> {
                    String key = "key" + i;
                    String value = "value" + i;
                    lsmEngine.put(new KVRecord(key, value));
                    int ccc = cntPut.incrementAndGet();
                    if (ccc % 100_000 == 0) System.err.println(ccc);

                };

                Time.withTime("put", () ->
                        TestUtils.runConcurrentTest(taskCount, 10000, 1, func1, numberOfThreads)
                );
            }

            lsmEngine.forceFlush();

            {
                AtomicInteger cntGet = new AtomicInteger(0);
                UnCheckedConsumer<Integer, IOException> func1 = i -> {
                    String key = "key" + i;
                    String value = "value" + i;
                    assertEquals(value, new String(lsmEngine.get(key).valueBytes));
                    int ccc = cntGet.incrementAndGet();
                    if (ccc % 100_000 == 0) System.err.println(ccc);
                };

                Time.withTime("get", () ->
                        TestUtils.runConcurrentTest(taskCount, 10000, 1, func1, numberOfThreads)
                );
            }

            lsmEngine.optimize();

            {
                AtomicInteger cntGet = new AtomicInteger(0);
                UnCheckedConsumer<Integer, IOException> func1 = i -> {
                    String key = "key" + i;
                    String value = "value" + i;
                    assertEquals(value, new String(lsmEngine.get(key).valueBytes));
                    int ccc = cntGet.incrementAndGet();
                    if (ccc % 100_000 == 0) System.err.println(ccc);
                };

                Time.withTime("get optimized", () ->
                        TestUtils.runConcurrentTest(taskCount, 10000, 1, func1, numberOfThreads)
                );
                {
                    Query query = new Query.QueryBuilder().withNoMinBound().withNoMaxBound().withNoLimit().build();
                    QueryIterator rangeRecords = lsmEngine.getRangeRecords(query);
                    int i = 0;
                    while (rangeRecords.hasNext()){
                        rangeRecords.next();
                        i++;
                    }
                    System.out.println("Range full: " + i);
                }

                {
                    Query query = new Query.QueryBuilder().withNoMinBound().withNoMaxBound().withLimit(20).build();
                    QueryIterator rangeRecords = lsmEngine.getRangeRecords(query);
                    int i = 0;
                    ArrayList<KVRecord> kvRecords = new ArrayList<>();
                    while (rangeRecords.hasNext()){
                        KVRecord next = rangeRecords.next();
                        kvRecords.add(next);
                        i++;
                    }
                    System.out.println("Range limit: " + i);
                    kvRecords.forEach(System.out::println);
                }


            }
        } catch (Throwable e) {
            System.err.println("here" + e);
            System.out.println();
            throw e;
        }

    }

    @Test
    public void testPutDeleteAndGet(@TempDir Path tmpPath) throws Exception {
//        int taskCount = 10_000_000;
        int taskCount = 2_000_000;
        int numberOfThreads = 200;
        String kvTableName = "test1";
        LsmConf lsmConf = new LsmConf(400, 100_000, 0.99);
        File tableDir = tmpPath.resolve(kvTableName).toFile();
        tableDir.mkdir();
        LsmEngine lsmEngine = LsmEngine.createNewTable("test_db.test1", tableDir, lsmConf, scheduler);

        {
            AtomicInteger cntPut = new AtomicInteger(0);
            UnCheckedConsumer<Integer, IOException> func1 = i -> {
                String key = "key" + i;
                String value = "value" + i;
                lsmEngine.put(new KVRecord(key, value));
                if (i % 2 == 0) lsmEngine.remove(key);
                int ccc = cntPut.incrementAndGet();
                if (ccc % 100_000 == 0) System.err.println(ccc);

            };

            Time.withTime("put", () ->
                    TestUtils.runConcurrentTest(taskCount, 10000, 1, func1, numberOfThreads)
            );
        }

        lsmEngine.forceFlush();
        SimpleMeterRegistry oneSimpleMeter = new SimpleMeterRegistry();
        CumulativeTimer notOptimizedTimer = (CumulativeTimer) Timer
                .builder("notOptimizedTimer")
                .publishPercentileHistogram(true)
                .publishPercentiles(0.1, 0.3, 0.5, 0.6, 0.7, 0.8, 0.9, 0.95, 0.99)
                .register(oneSimpleMeter);


//        Timer timer321 = oneSimpleMeter.timer("timer321");
        Time.mes = true;
        {
            AtomicInteger cntGet = new AtomicInteger(0);
            UnCheckedConsumer<Integer, IOException> func1 = i -> {
                Time.withTimer(notOptimizedTimer, () -> {
                    String key = "key" + i;
                    String value = "value" + i;
                    if (i % 2 == 0) {
                        assertNull(lsmEngine.get(key));
                    } else {
                        assertEquals(value, new String(lsmEngine.get(key).valueBytes));
                    }

                    int ccc = cntGet.incrementAndGet();
                    if (ccc % 100_000 == 0) System.err.println(ccc);
                });
            };

            Time.withTime("get", () ->
                    TestUtils.runConcurrentTest(taskCount, 10000, 1, func1, numberOfThreads)
            );
        }

        HistogramSnapshot histogramSnapshot = notOptimizedTimer.takeSnapshot();
        System.out.println(histogramSnapshot);
        System.out.println(notOptimizedTimer.totalTime(TimeUnit.SECONDS));

        lsmEngine.optimize();

        CumulativeTimer optimizedTimer = (CumulativeTimer) Timer
                .builder("optimizedTimer")
                .publishPercentileHistogram(true)
                .publishPercentiles(0.1, 0.3, 0.5, 0.6, 0.7, 0.8, 0.9, 0.95, 0.99)
                .register(oneSimpleMeter);

        {
            AtomicInteger cntGet = new AtomicInteger(0);
            UnCheckedConsumer<Integer, IOException> func1 = i -> {
                Time.withTimer(optimizedTimer, () -> {
                    String key = "key" + i;
                    String value = "value" + i;
                    if (i % 2 == 0) {
                        assertNull(lsmEngine.get(key));
                    } else {
                        assertEquals(value, new String(lsmEngine.get(key).valueBytes));
                    }
                    int ccc = cntGet.incrementAndGet();
                    if (ccc % 100_000 == 0) System.err.println(ccc);
                });
            };

            Time.withTime("get optimized", () ->
                    TestUtils.runConcurrentTest(taskCount, 10000, 1, func1, numberOfThreads)
            );

            HistogramSnapshot histogramSnapshot2 = optimizedTimer.takeSnapshot();
            System.out.println(histogramSnapshot2);
            System.out.println(optimizedTimer.totalTime(TimeUnit.SECONDS));
        }

    }

}
