package perf_test;


import com.keks.kv_storage.KVStore;
import com.keks.kv_storage.bplus.conf.BPlusConfParamsEnum;
import com.keks.kv_storage.conf.TableEngineType;
import com.keks.kv_storage.utils.Time;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.cumulative.CumulativeTimer;
import io.micrometer.core.instrument.distribution.HistogramSnapshot;
import io.micrometer.core.instrument.distribution.ValueAtPercentile;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import utils.ShowAsTable;

import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;


@Disabled
public class BplusPerfTest {

    @Test
    public void ex1() throws ExecutionException, InterruptedException, TimeoutException {

        int numberOfThreads = 100;
        int numberOfRecords = 100_000;
//        int numberOfRecords = 5;
        SimpleMeterRegistry oneSimpleMeter = new SimpleMeterRegistry();

        Instant start = Instant.now();


        CumulativeTimer optimizedTimer = (CumulativeTimer) Timer
                .builder("optimizedTimer")
                .publishPercentileHistogram(true)
                .publishPercentiles(0.5, 0.75, 0.99, 0.999, 0.9999)
                .register(oneSimpleMeter);
        Time.mes = true;
        Consumer<Integer> func = i -> {
            try {
                Time.withTimerMillis(optimizedTimer, () -> {
                            Thread.sleep(new Random().nextInt(10));
//                            Thread.sleep(1000);
                        }
                );

            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        };

        runConcurrentTest(
                numberOfRecords,
                func,
                numberOfThreads);

        Instant finish = Instant.now();
        Duration between = Duration.between(start, finish);

        printStats(numberOfThreads, between.toSeconds(), optimizedTimer);
    }

    private static void printStats(int threads, long seconds, CumulativeTimer timer) {
        ArrayList<String> elems = new ArrayList<>();
        ArrayList<String> headers = new ArrayList<>();
        HistogramSnapshot histogramSnapshot = timer.takeSnapshot();

        headers.add("threads");
        elems.add(String.valueOf(threads));

        headers.add("records");
        elems.add(String.valueOf(timer.count()));

        headers.add("total sec");
        elems.add(String.valueOf(seconds));

        headers.add("mean millis");
        elems.add(String.valueOf(String.format("%.3f", timer.mean(TimeUnit.MILLISECONDS))));

        headers.add("ops/sec");
        elems.add(String.valueOf(String.format("%.3f", timer.count() / (double) seconds)));

        for (ValueAtPercentile valueAtPercentile : histogramSnapshot.percentileValues()) {
            headers.add("p" + valueAtPercentile.percentile() + " millis");
            elems.add(String.valueOf(String.format("%.3f", valueAtPercentile.value(TimeUnit.MILLISECONDS))));
        }

        System.out.println(ShowAsTable.show(new ArrayList<>() {{
            add(elems);
        }}, headers));

    }

    private static void printStatsCsv(int threads, long seconds, CumulativeTimer timer) {
        ArrayList<String> elems = new ArrayList<>();
        ArrayList<String> headers = new ArrayList<>();
        HistogramSnapshot histogramSnapshot = timer.takeSnapshot();

        headers.add("threads");
        elems.add(String.valueOf(threads));

        headers.add("records");
        elems.add(String.valueOf(timer.count()));

        headers.add("total sec");
        elems.add(String.valueOf(seconds));

        headers.add("mean millis");
        elems.add(String.valueOf(String.format("%.3f", timer.mean(TimeUnit.MILLISECONDS))));

        headers.add("ops/sec");
        elems.add(String.valueOf(String.format("%.3f", timer.count() / (double) seconds)));

        for (ValueAtPercentile valueAtPercentile : histogramSnapshot.percentileValues()) {
            headers.add("p" + valueAtPercentile.percentile() + " millis");
            elems.add(String.valueOf(String.format("%.3f", valueAtPercentile.value(TimeUnit.MILLISECONDS))));
        }

        System.out.println(String.join(",", elems));

    }

    @Test
    public void exPut(@TempDir Path tmpPath) throws InterruptedException, ExecutionException, TimeoutException {
        Time.mes = true;
        List<Integer> threadsList = Arrays.asList(1, 5, 10, 15, 20, 25, 30, 50, 100, 150, 200);
//        List<Integer> threadsList = Arrays.asList(15, 20, 25, 30, 50, 100, 150, 200);
        for (Integer threadNum : threadsList) {
            Path resolve = tmpPath.resolve(String.valueOf(threadNum));
            resolve.toFile().mkdir();
            Instant start = Instant.now();
            CumulativeTimer timer = runPut(threadNum, 1_000_000, resolve);
            Instant finish = Instant.now();
            Duration between = Duration.between(start, finish);
            printStatsCsv(threadNum, between.toSeconds(), timer);
        }

    }

    private CumulativeTimer runPut(int numberOfThreads, int numberOfRecords, Path tmpPath) throws ExecutionException, InterruptedException, TimeoutException {
        SimpleMeterRegistry oneSimpleMeter = new SimpleMeterRegistry();
        CumulativeTimer optimizedTimer = (CumulativeTimer) Timer
                .builder("optimizedTimer")
                .publishPercentileHistogram(true)
                .publishPercentiles(0.1, 0.5, 0.75, 0.99, 0.999, 0.9999)
                .register(oneSimpleMeter);

        String dbName = "bplus_test_db";
        String tblName = "bplus_table_test";

        AtomicInteger cnt = new AtomicInteger();
        try (KVStore kvStore = new KVStore(tmpPath.toFile());) {
            kvStore.createDB(dbName);
            Properties properties = new Properties() {{
                put(BPlusConfParamsEnum.BTREE_ORDER, 400);
                put(BPlusConfParamsEnum.BTREE_PAGE_BUFFER_SIZE_BYTES, 400_000_000L);
            }};
            kvStore.createTable(
                    dbName,
                    tblName,
                    TableEngineType.BPLUS.toString(),
                    properties);

            Consumer<Integer> func = i -> {
                String key = "key" + i;
                String value = "value" + i;


                Time.withTimer(optimizedTimer, () -> {
                            // adding record
                            kvStore.put(dbName, tblName, key, value.getBytes());
                        }
                );


//                // getting record
//                assertEquals(value, new String(kvStore.get(dbName, tblName, key)));
//
//                // remove each third record
//                if (i % 3 == 0) {
//                    kvStore.remove(dbName, tblName, key);
//                    assertNull(kvStore.get(dbName, tblName, key));
//                }

//                int i1 = cnt.incrementAndGet();
//                if (i1 % 10_000 == 0) {
//                    System.out.println(i1);
//                }

            };

            runConcurrentTest(
                    numberOfRecords,
                    func,
                    numberOfThreads);

            return optimizedTimer;

        }
    }

    @Test
    public void exGet(@TempDir Path tmpPath) throws InterruptedException, ExecutionException, TimeoutException {
        Time.mes = true;
        List<Integer> threadsList = Arrays.asList(1, 5, 10, 15, 20, 25, 30, 50, 100, 150, 200);
//        List<Integer> threadsList = Arrays.asList(15, 20, 25, 30, 50, 100, 150, 200);

        String dbName = "bplus_test_db";
        String tblName = "bplus_table_test";
        try (KVStore kvStore = new KVStore(tmpPath.toFile());) {
            kvStore.createDB(dbName);
            Properties properties = new Properties() {{
                put(BPlusConfParamsEnum.BTREE_ORDER, 400);
                put(BPlusConfParamsEnum.BTREE_PAGE_BUFFER_SIZE_BYTES, 400_000_000L);
            }};
            kvStore.createTable(
                    dbName,
                    tblName,
                    TableEngineType.BPLUS.toString(),
                    properties);

            Consumer<Integer> func = i -> {
                String key = "key" + i;
                String value = "value" + i;

                kvStore.put(dbName, tblName, key, value.getBytes());

            };

            runConcurrentTest(
                    1_000_000,
                    func,
                    15);

        }

        for (Integer threadNum : threadsList) {
            Instant start = Instant.now();
            CumulativeTimer timer = runGet(threadNum, 1_000_000, tmpPath);
            Instant finish = Instant.now();
            Duration between = Duration.between(start, finish);
            printStatsCsv(threadNum, between.toSeconds(), timer);
        }

    }

    private CumulativeTimer runGet(int numberOfThreads, int numberOfRecords, Path tmpPath) throws ExecutionException, InterruptedException, TimeoutException {
        String dbName = "bplus_test_db";
        String tblName = "bplus_table_test";


        SimpleMeterRegistry oneSimpleMeter = new SimpleMeterRegistry();
        CumulativeTimer optimizedTimer = (CumulativeTimer) Timer
                .builder("optimizedTimer")
                .publishPercentileHistogram(true)
                .publishPercentiles(0.1, 0.5, 0.75, 0.99, 0.999, 0.9999)
                .register(oneSimpleMeter);


        AtomicInteger cnt = new AtomicInteger();
        try (KVStore kvStore = new KVStore(tmpPath.toFile());) {

            Consumer<Integer> func = i -> {
                String key = "key" + i;
                String value = "value" + i;

                Time.withTimer(optimizedTimer, () -> {
                            assertEquals(value, new String(kvStore.get(dbName, tblName, key)));
                        }
                );

            };

            runConcurrentTest(
                    numberOfRecords,
                    func,
                    numberOfThreads);

            return optimizedTimer;

        }
    }

    @Test
    public void exRemove(@TempDir Path tmpPath) throws InterruptedException, ExecutionException, TimeoutException {
        Time.mes = true;
        List<Integer> threadsList = Arrays.asList(1, 5, 10, 15, 20, 25, 30, 50, 100, 150, 200);

        for (Integer threadNum : threadsList) {
            Path resolve = tmpPath.resolve(String.valueOf(threadNum));
            resolve.toFile().mkdir();
            Instant start = Instant.now();
            CumulativeTimer timer = runRemove(threadNum, 1_000_000, resolve);
            Instant finish = Instant.now();
            Duration between = Duration.between(start, finish);
            printStatsCsv(threadNum, between.toSeconds(), timer);
        }

    }

    private CumulativeTimer runRemove(int numberOfThreads, int numberOfRecords, Path tmpPath) throws ExecutionException, InterruptedException, TimeoutException {
        String dbName = "bplus_test_db";
        String tblName = "bplus_table_test";


        try (KVStore kvStore = new KVStore(tmpPath.toFile())) {
            kvStore.createDB(dbName);
            Properties properties = new Properties() {{
                put(BPlusConfParamsEnum.BTREE_ORDER, 400);
                put(BPlusConfParamsEnum.BTREE_PAGE_BUFFER_SIZE_BYTES, 400_000_000L);
            }};
            kvStore.createTable(
                    dbName,
                    tblName,
                    TableEngineType.BPLUS.toString(),
                    properties);

            Consumer<Integer> func = i -> {
                String key = "key" + i;
                String value = "value" + i;
                // adding record
                kvStore.put(dbName, tblName, key, value.getBytes());
            };
            runConcurrentTest(
                    numberOfRecords,
                    func,
                    15);
        }



        SimpleMeterRegistry oneSimpleMeter = new SimpleMeterRegistry();
        CumulativeTimer optimizedTimer = (CumulativeTimer) Timer
                .builder("optimizedTimer")
                .publishPercentileHistogram(true)
                .publishPercentiles(0.1, 0.5, 0.75, 0.99, 0.999, 0.9999)
                .register(oneSimpleMeter);


        AtomicInteger cnt = new AtomicInteger();
        try (KVStore kvStore = new KVStore(tmpPath.toFile());) {

            Consumer<Integer> func = i -> {
                String key = "key" + i;

                Time.withTimer(optimizedTimer, () -> {
                            kvStore.remove(dbName, tblName, key);
                        }
                );

            };

            runConcurrentTest(
                    numberOfRecords,
                    func,
                    numberOfThreads);

            return optimizedTimer;

        }
    }


    public static void runConcurrentTest(int taskCount,
                                         Consumer<Integer> function,
                                         int numberOfThreads) throws InterruptedException, ExecutionException, TimeoutException {
        ExecutorService executor = Executors.newFixedThreadPool(numberOfThreads);
        List<Future<?>> futures = new ArrayList<>(taskCount);
        for (int i = 0; i < taskCount; i++) {
            int y = i;
            Future<?> future1 = executor.submit(() -> function.accept(y));
            futures.add(future1);
        }
        executor.shutdown();
        if (!executor.awaitTermination(10000, TimeUnit.SECONDS)) {
            throw new InterruptedException();
        }

        assertEquals(taskCount, futures.size());
        for (Future<?> future : futures) {
            future.get(1, TimeUnit.SECONDS);
        }

    }


}
