package com.keks.kv_storage.lsm;

import com.keks.kv_storage.record.KVRecord;
import com.keks.kv_storage.KVTableStatistics;
import com.keks.kv_storage.conf.Params;
import com.keks.kv_storage.conf.TableEngine;
import com.keks.kv_storage.io.FileUtils;
import com.keks.kv_storage.lsm.conf.LsmConf;
import com.keks.kv_storage.lsm.io.LsmConfJsonRW;
import com.keks.kv_storage.lsm.query.SsTableRangeIterator;
import com.keks.kv_storage.lsm.ss_table.SSTablesManager;
import com.keks.kv_storage.lsm.query.LsmRecordsIterator;
import com.keks.kv_storage.query.Query;
import com.keks.kv_storage.query.QueryIterator;
import com.keks.kv_storage.query.range.RangeKey;
import com.keks.kv_storage.utils.SimpleScheduler;
import com.keks.kv_storage.utils.Time;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.cumulative.CumulativeTimer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;


public class LsmEngine extends TableEngine {

    public static final SimpleMeterRegistry oneSimpleMeter = new SimpleMeterRegistry();

    static final Logger log = LogManager.getLogger(LsmEngine.class.getName());

    private final ReentrantLock spillLock = new ReentrantLock();
    private final ReentrantLock flushLock = new ReentrantLock();

    public final String tableName;
    private final SSTablesManager ssTablesManager;

    public final LsmConf lsmConf;

    public final File kvTableDirPath;

    private final String tableId;
    private final MemCacheTable memCacheTable;

    public static LsmEngine createNewTable(String tableId,
                                           File kvTableDirPath,
                                           LsmConf lsmConf,
                                           SimpleScheduler scheduler) throws IOException {
        try (LsmConfJsonRW lsmConfJsonRW = new LsmConfJsonRW(kvTableDirPath)) {
            lsmConfJsonRW.write(lsmConf);
        }
        return new LsmEngine(tableId, kvTableDirPath, lsmConf, new SSTablesManager(tableId, kvTableDirPath, lsmConf, scheduler));
    }

    public static LsmEngine loadTable(String tableId,
                                      File kvTableDirPath,
                                      SimpleScheduler scheduler) throws IOException {
        try (LsmConfJsonRW lsmConfJsonRW = new LsmConfJsonRW(kvTableDirPath)) {
            LsmConf lsmConf = lsmConfJsonRW.read();
            SSTablesManager ssTablesManager = SSTablesManager.loadSSTableManager(tableId, kvTableDirPath, lsmConf, scheduler);
            return new LsmEngine(tableId, kvTableDirPath, lsmConf, ssTablesManager);
        }
    }

    private LsmEngine(String tableId,
                      File kvTableDirPath,
                      LsmConf lsmConf,
                      SSTablesManager ssTablesManager) {
        this.tableId = tableId;
        this.memCacheTable = new MemCacheTable(lsmConf.memCacheSize);
        this.lsmConf = lsmConf;
        this.tableName = kvTableDirPath.getName();
        this.kvTableDirPath = kvTableDirPath;
        this.ssTablesManager = ssTablesManager;
    }

    @Override
    public void putKV(byte[] key, byte[] value) throws IOException {
        memCacheTable.put(new String(key), new KVRecord(new String(key), value));
        spillMemCacheToDiskIfFull();
    }

    public static AtomicInteger recordsCnt = new AtomicInteger();
    public static AtomicInteger recordsGetCnt = new AtomicInteger();

    @Override
    public void put(KVRecord kvRecord) throws IOException {
        int i = recordsCnt.incrementAndGet();
        if (i % 100_000 == 0) {
            System.out.println("Lsm inserted: " + i);
        }
        Time.withTimer(putOptimizedTimer, () -> {
            memCacheTable.put(kvRecord.key, kvRecord);
        });
        spillMemCacheToDiskIfFull();
    }

    @Override
    public void putBatch(ArrayList<KVRecord> kvRecords) throws IOException {
        for (KVRecord kvRecord : kvRecords) {
            put(kvRecord);
        }
    }

    @Override
    public void putBatch(Iterator<KVRecord> kvRecords) throws IOException {
        while (kvRecords.hasNext()) {
            put(kvRecords.next());
        }
    }

    public void put(String key, String value) throws IOException {
        put(new KVRecord(key, value.getBytes()));
    }

    @Override
    public void removeBatch(ArrayList<KVRecord> kvRecords) throws IOException {
        for (KVRecord kvRecord : kvRecords) {
            remove(kvRecord.key);
        }
    }

    public void removeBatch(Iterator<KVRecord> kvRecords) throws IOException {
        while (kvRecords.hasNext()) {
            remove(kvRecords.next().key);
        }
    }

    public static CumulativeTimer putOptimizedTimer = (CumulativeTimer) Timer
            .builder("optimizedTimer")
            .publishPercentileHistogram(true)
            .publishPercentiles(0.5, 0.75, 0.99, 0.999, 0.9999)
            .register(oneSimpleMeter);

    public static CumulativeTimer removeOptimizedTimer = (CumulativeTimer) Timer
            .builder("optimizedTimer")
            .publishPercentileHistogram(true)
            .publishPercentiles(0.5, 0.75, 0.99, 0.999, 0.9999)
            .register(oneSimpleMeter);

    @Override
    public void remove(String key) throws IOException {
        Time.withTimer(removeOptimizedTimer, () -> {
            memCacheTable.remove(key);
        });
        spillMemCacheToDiskIfFull();
    }


    @Override
    public void forceFlush() throws IOException {
        try {
            flushLock.lock();
            flush();
        } finally {
            flushLock.unlock();
        }
    }

    @Override
    public Params<?> getTableProperties() {
        return lsmConf;
    }

    @Override
    public long getRecordsCnt(Query query) throws IOException {
        try (QueryIterator rangeRecords = getRangeRecords(query)) {
            long cnt = 0;
            while (rangeRecords.hasNext()) {
                rangeRecords.next();
                cnt++;
            }
            return cnt;
        }

    }

    @Override
    public void optimize() throws IOException {
        if (1 < ssTablesManager.getSStableCnt()) {
            log.info("merging all");
            log.info("Table: " + tableName + " Compacting records on disk...");
            ssTablesManager.mergeAllTables();
            log.info("Table: " + tableName + " Compacting finished");
        }
    }

    @Override
    public KVRecord get(String key) throws IOException {
        KVRecord res = memCacheTable.get(key);
        if (res == null) {
            res = ssTablesManager.searchKey(key);
        }
        int i = recordsGetCnt.incrementAndGet();
        if (i % 100_000 == 0) System.out.println(i);
        if (res == null || res.isDeleted()) {
            return null;
        } else {
            return res;
        }
    }

    protected QueryIterator getRangeRecords(RangeKey left, RangeKey right) throws IOException {
        return getRangeRecords(new Query.QueryBuilder().withMinKey(left).withMaxKey(right).build());
    }

    @Override
    public QueryIterator getRangeRecords(Query query) throws IOException {
        List<SsTableRangeIterator> allParts = memCacheTable
                .getAllParts(query.min, query.max)
                .stream()
                .map(iter -> new SsTableRangeIterator(
                                iter,
                                query.min,
                                query.max, () -> {
                        })
                ).collect(Collectors.toList());
        allParts.addAll(ssTablesManager.searchRange(query.min, query.max));
        return new QueryIterator(new LsmRecordsIterator(allParts), query);
    }

    @Override
    public void close() throws IOException {
        forceFlush();
        ssTablesManager.close();
    }

    private KVTableStatistics getStatistics() {
        double directorySizeInMBytes;
        directorySizeInMBytes = FileUtils.getDirectorySizeInBytes(kvTableDirPath.toPath()) / 1024.0 / 1024.0;
        return new KVTableStatistics(
                tableName,
                memCacheTable.getApproxRecCnt(),
//                memCacheTable.getDelRecCnt(), TODO
                0,
                directorySizeInMBytes,
                ssTablesManager.ssTablesSortedList.toJavaList(),
                lsmConf);
    }

    private void spillMemCacheToDiskIfFull() throws IOException { // TODO create global spillCache thread
        if (memCacheTable.isFull() && !memCacheTable.isFlushing() && spillLock.tryLock()) {
            if (memCacheTable.isFull() && !memCacheTable.isFlushing()) {
                if (lsmConf.syncWithThreadFlush) {
                    try {
                        flushLock.lock();
                        if (memCacheTable.isFull()) {
                            System.out.println("fdfdf");
                            log.info("spilling to disk");
                            Time.withTime("put", this::flush
                            );
                        }
                    } finally {
                        flushLock.unlock();
                    }
                } else {
                    ReentrantLock waitFlushStartedLock = new ReentrantLock();
                    Condition flushStartedCondition = waitFlushStartedLock.newCondition();
                    waitFlushStartedLock.lock();
                    Runnable runnable = () -> {
                        try {
                            flushLock.lock();
                            memCacheTable.setIsFlushing();
                            System.out.println("waiting2");
                            waitFlushStartedLock.lock();
                            flushStartedCondition.signalAll();
                            waitFlushStartedLock.unlock();
                            System.out.println("running2");

                            if (memCacheTable.isFull()) {
                                System.out.println("fdfdf");
                                log.info("spilling to disk");
                                Time.withTime("flush", this::flush
                                );
                            }
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        } finally {
                            memCacheTable.unsetIsFlushing();
                            flushLock.unlock();
                        }
                    };
                    Thread thread = new Thread(runnable);
                    thread.start();

                    try {

                        System.out.println("waiting1");
                        flushStartedCondition.await();
                        waitFlushStartedLock.unlock();
                        System.out.println("running1");

                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            }

            spillLock.unlock();
        }
    }

    private void flush() throws IOException {
        if (!memCacheTable.isEmpty()) {
            log.info("Table: " + tableName + " Flushing records to disk...");
            int approxRecCnt = memCacheTable.getApproxRecCnt();
            memCacheTable.addNewPartition();
            memCacheTable.setOldestIsFlushed();
            Iterator<KVRecord> oldestPartitionIter = memCacheTable.getOldestPartitionIter();
            ArrayList<KVRecord> objectLinkedList = new ArrayList<>();
            while (oldestPartitionIter.hasNext()) {
                objectLinkedList.add(oldestPartitionIter.next());
            }

//            System.out.println("Flushing " + collect);
            ssTablesManager.createSSTableOnDisk(memCacheTable.getOldestPartitionIter(), approxRecCnt);
            log.info("Table: " + tableName + " Flushing finished");
            memCacheTable.dropOldestPartition();
        }
    }

    protected QueryIterator getAll() throws IOException {
        return getRangeRecords(Query.QUERY_ALL);
    }

    protected void mergeNLastSSTables(int number) throws IOException {
        ssTablesManager.mergeNLastSSTables(number);
    }

}
