package perf_test;


import com.keks.kv_storage.KVStore;
import com.keks.kv_storage.conf.TableEngineType;
import com.keks.kv_storage.kv_table_conf.KvTableConfParamsEnum;
import com.keks.kv_storage.lsm.LsmEngine;
import com.keks.kv_storage.lsm.conf.LsmConfParamsEnum;
import com.keks.kv_storage.record.KVRecord;
import com.keks.kv_storage.utils.Time;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.cumulative.CumulativeTimer;
import io.micrometer.core.instrument.distribution.HistogramSnapshot;
import io.micrometer.core.instrument.distribution.ValueAtPercentile;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.CleanupMode;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Disabled
public class LsmPerfTest {

    @Test
    public void ex0() {
        int i = 1;
        String key = "key" + String.format("%08d", i);
        String value = "value" + String.format("%08d", i);
        KVRecord kvRecord = new KVRecord(key, value);
        System.out.println(key.length() + kvRecord.getLen());

    }

    @Test
    public void ex1(@TempDir Path tmpPath) throws Exception {
        int numberOfRecords = 5_000_000;
        List<Integer> list = IntStream.range(0, numberOfRecords).boxed().collect(Collectors.toList());
        Collections.shuffle(list);
        String dbName = "lsm_test_db";
        String tblName = "lsm_table_test";


        try (KVStore kvStore = new KVStore(tmpPath.toFile())) {
            kvStore.createDB(dbName);
            Properties properties = new Properties() {{
                put(LsmConfParamsEnum.MEM_CACHE_SIZE_RECORDS, 1_000_000);
                put(LsmConfParamsEnum.BLOOM_FILTER_FALSE_POSITIVE_RATE, 0.1);
            }};
            kvStore.createTable(
                    dbName,
                    tblName,
                    TableEngineType.LSM.toString(),
                    properties);

            Consumer<Integer> func = x -> {
                Integer i = list.get(x);
                String key = "key" + String.format("%08d", i);
                String value = "value" + String.format("%08d", i);
                // adding record
                kvStore.put(dbName, tblName, key, value.getBytes());

            };


            int numberOfThreads = 15;
            runConcurrentTest(
                    numberOfRecords,
                    func,
                    numberOfThreads);

        }

    }

    public static String printStatsCsv(int threads, long millis, CumulativeTimer timer, long writtenBytes) {
        ArrayList<String> elems = new ArrayList<>();
        ArrayList<String> headers = new ArrayList<>();
        HistogramSnapshot histogramSnapshot = timer.takeSnapshot();

        headers.add("threads");
        elems.add(String.valueOf(threads));

        headers.add("records");
        elems.add(String.valueOf(timer.count()));

        headers.add("written mb");
        elems.add(String.valueOf(writtenBytes / 1024));

        headers.add("avg record size bytes");
        elems.add(String.valueOf(writtenBytes / timer.count()));

        headers.add("total sec");
        elems.add(String.valueOf(String.format("%.1f", (millis / 1_000.0))));

        headers.add("mean millis");
        elems.add(String.valueOf(String.format("%.3f", timer.mean(TimeUnit.MILLISECONDS))));

        headers.add("ops/sec");
        elems.add(String.valueOf((int) (timer.count() / (millis / 1_000.0))));

        for (ValueAtPercentile valueAtPercentile : histogramSnapshot.percentileValues()) {
            headers.add("p" + valueAtPercentile.percentile() + " millis");
            elems.add(String.valueOf(String.format("%.3f", valueAtPercentile.value(TimeUnit.MILLISECONDS))));
        }

        System.out.println(String.join(",", headers));
        System.out.println(String.join(",", elems));
        return String.join(",", elems);
    }

    // memcache 50mb
    @Test
    public void exPut(@TempDir(cleanup = CleanupMode.ALWAYS) Path tmpPath) throws InterruptedException, ExecutionException, TimeoutException {
        Time.mes = true;
        List<Integer> threadsList = Arrays.asList(1, 4, 8, 16, 32, 50, 100, 160, 200);
//        List<Integer> threadsList = Arrays.asList(1);
//        List<Integer> threadsList = Arrays.asList(15, 20, 25, 30, 50, 100, 150, 200);
        StringBuilder sb = new StringBuilder();
        for (Integer threadNum : threadsList) {
            Path resolve = tmpPath.resolve(String.valueOf(threadNum));
            resolve.toFile().mkdir();
            Instant start = Instant.now();
            DataGenerator dataGenerator = new DataGenerator(5_000_000, threadNum);
            CumulativeTimer timer = runPut(threadNum, dataGenerator, resolve);
            Instant finish = Instant.now();
            Duration between = Duration.between(start, finish);
            String metr = printStatsCsv(threadNum, between.toMillis(), timer, dataGenerator.getTotalBytes());
            sb.append(metr).append("\n");
        }
        System.out.println(sb);

    }

    private CumulativeTimer runPut(int numberOfThreads, DataGenerator dataGenerator, Path tmpPath) throws ExecutionException, InterruptedException, TimeoutException {

        String dbName = "lsm_test_db";
        String tblName = "lsm_table_test";

        SimpleMeterRegistry oneSimpleMeter = new SimpleMeterRegistry();

        LsmEngine.putOptimizedTimer = (CumulativeTimer) Timer
                .builder("optimizedTimer")
                .publishPercentileHistogram(true)
                .publishPercentiles(0.5, 0.75, 0.99, 0.999, 0.9999)
                .register(oneSimpleMeter);

        try (KVStore kvStore = new KVStore(tmpPath.toFile())) {
            kvStore.createDB(dbName);
            Properties properties = new Properties() {{
//                put(LsmConfParamsEnum.MEM_CACHE_SIZE, 1_500_000);
                put(LsmConfParamsEnum.MEM_CACHE_SIZE_RECORDS, 524288);
                put(LsmConfParamsEnum.SPARSE_INDEX_SIZE_RECORDS, 8192);
                put(KvTableConfParamsEnum.ENABLE_WAL, false);
                put(LsmConfParamsEnum.BLOOM_FILTER_FALSE_POSITIVE_RATE, 0.1);
            }};
            kvStore.createTable(
                    dbName,
                    tblName,
                    TableEngineType.LSM.toString(),
                    properties);

            Consumer<Integer> func = i -> {
                Iterator<KVRecord> batchIter = dataGenerator.getBatchIter(i);
                kvStore.put(dbName, tblName, batchIter);
            };

            runConcurrentTest(
                    numberOfThreads,
                    func,
                    numberOfThreads);

            kvStore.dropTable(dbName, tblName);
            return LsmEngine.putOptimizedTimer;
        }

    }

    @Test
    public void exGet(@TempDir(cleanup = CleanupMode.ALWAYS) Path tmpPath) throws InterruptedException, ExecutionException, TimeoutException {
        int numberOfRecords = 1_000_000;
        Time.mes = true;
        List<Integer> threadsList = Arrays.asList(1, 4, 8, 16, 32, 50, 100, 160, 200);
//        List<Integer> threadsList = Arrays.asList(1);

        String dbName = "lsm_test_db";
        String tblName = "lsm_table_test";

        StringBuilder sb = new StringBuilder();
        DataGenerator dataGenerator = new DataGenerator(numberOfRecords, 1);
        try (KVStore kvStore = new KVStore(tmpPath.toFile())) {
            kvStore.createDB(dbName);
            Properties properties = new Properties() {{
                put(LsmConfParamsEnum.SPARSE_INDEX_SIZE_RECORDS, 8192);
                put(LsmConfParamsEnum.MEM_CACHE_SIZE_RECORDS, 2_000_000);
                put(LsmConfParamsEnum.BLOOM_FILTER_FALSE_POSITIVE_RATE, 0.1);
            }};
            kvStore.createTable(
                    dbName,
                    tblName,
                    TableEngineType.LSM.toString(),
                    properties);

            Iterator<KVRecord> batchIter = dataGenerator.getBatchIter(0);
            kvStore.put(dbName, tblName, batchIter);

            kvStore.flushTable(dbName, tblName);
            kvStore.optimizeTable(dbName, tblName);

        }

        for (Integer threadNum : threadsList) {
            Instant start = Instant.now();
            CumulativeTimer timer = runGet(threadNum, numberOfRecords, tmpPath);
            Instant finish = Instant.now();
            Duration between = Duration.between(start, finish);
            String metr = printStatsCsv(threadNum, between.toMillis(), timer, dataGenerator.getTotalBytes());
            sb.append(metr).append("\n");
        }
        System.out.println(sb);

    }

    private CumulativeTimer runGet(int numberOfThreads, int numberOfRecords, Path tmpPath) throws ExecutionException, InterruptedException, TimeoutException {
        String dbName = "lsm_test_db";
        String tblName = "lsm_table_test";


        SimpleMeterRegistry oneSimpleMeter = new SimpleMeterRegistry();
        CumulativeTimer optimizedTimer = (CumulativeTimer) Timer
                .builder("optimizedTimer")
                .publishPercentileHistogram(true)
                .publishPercentiles(0.5, 0.75, 0.99, 0.999, 0.9999)
                .register(oneSimpleMeter);


        AtomicInteger cnt = new AtomicInteger();
        try (KVStore kvStore = new KVStore(tmpPath.toFile());) {

            Consumer<Integer> func = i -> {
                String key = "key" + String.format("%09d", i);

                Time.withTimer(optimizedTimer, () -> {
                            String s = new String(kvStore.get(dbName, tblName, key));
                            int x = 1;
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
            CumulativeTimer timer = runRemove(threadNum, 5_000_000, resolve);
            Instant finish = Instant.now();
            Duration between = Duration.between(start, finish);
            printStatsCsv(threadNum, between.toMillis(), timer, 1);
        }

    }

    private CumulativeTimer runRemove(int numberOfThreads, int numberOfRecords, Path tmpPath) throws ExecutionException, InterruptedException, TimeoutException {
        String dbName = "lsm_test_db";
        String tblName = "lsm_table_test";


        try (KVStore kvStore = new KVStore(tmpPath.toFile())) {
            kvStore.createDB(dbName);
            Properties properties = new Properties() {{
                put(LsmConfParamsEnum.MEM_CACHE_SIZE_RECORDS, 1_000_000);
                put(LsmConfParamsEnum.BLOOM_FILTER_FALSE_POSITIVE_RATE, 0.1);
            }};
            kvStore.createTable(
                    dbName,
                    tblName,
                    TableEngineType.LSM.toString(),
                    properties);

            Consumer<Integer> func = i -> {
                String key = "key" + String.format("%08d", i);
                String value = "value" + String.format("%08d", i);
                // adding record
                kvStore.put(dbName, tblName, key, value.getBytes());
            };
            runConcurrentTest(
                    numberOfRecords,
                    func,
                    numberOfThreads);
        }


        SimpleMeterRegistry oneSimpleMeter = new SimpleMeterRegistry();
        CumulativeTimer optimizedTimer = (CumulativeTimer) Timer
                .builder("optimizedTimer")
                .publishPercentileHistogram(true)
                .publishPercentiles(0.5, 0.75, 0.99, 0.999, 0.9999)
                .register(oneSimpleMeter);


        AtomicInteger cnt = new AtomicInteger();
        try (KVStore kvStore = new KVStore(tmpPath.toFile());) {

            Consumer<Integer> func = i -> {
                String key = "key" + String.format("%08d", i);

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
            future.get(10, TimeUnit.SECONDS);
        }

    }


}
