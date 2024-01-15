package com.keks.kv_storage.lsm.ss_table;

import com.keks.kv_storage.record.KVRecord;
import com.keks.kv_storage.lsm.conf.LsmConf;
import com.keks.kv_storage.lsm.io.BloomFilterRafRW;
import com.keks.kv_storage.lsm.io.MetadataJsonRW;
import com.keks.kv_storage.lsm.io.SSTableReader;
import com.keks.kv_storage.lsm.io.SSTableWriter;
import com.keks.kv_storage.lsm.query.SsTableRangeIterator;
import com.keks.kv_storage.lsm.query.LsmRecordsIterator;
import com.keks.kv_storage.lsm.utils.BloomFilter;
import com.keks.kv_storage.io.FileUtils;
import com.keks.kv_storage.query.range.RangeKey;
import com.keks.kv_storage.utils.Scheduler;
import com.keks.kv_storage.utils.SimpleScheduler;
import com.keks.kv_storage.utils.UnCheckedFunction;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import java.util.stream.Stream;


public class SSTablesManager {

    static final Logger log = LogManager.getLogger(SSTablesManager.class.getName());
    private final ReentrantLock ssTablesTailMergeLock = new ReentrantLock();

    public final LsmConf lsmConf;
    public final SStablesList ssTablesSortedList;

    private final AtomicInteger ssTableLastId;
    private final AtomicInteger ssTablesCnt;

    private final AtomicInteger recordsCnt;
    private final AtomicInteger deletedRecordsCnt;

    private final File tableDir;
    private final String logPrefix;
    private final String tableId;
    private ScheduledFuture<?> optimizeRunnable;
    private final SimpleScheduler scheduler;

    public SSTablesManager(String tableId, File tableDir, LsmConf lsmConf, SimpleScheduler scheduler) {
        this(tableId, tableDir, lsmConf, new LinkedList<>(), scheduler);
    }

    public static SSTablesManager loadSSTableManager(String tableId, File tableDir, LsmConf lsmConf, SimpleScheduler scheduler) throws IOException {
        try (Stream<Path> list = Files.list(tableDir.toPath())) {
            List<Path> collect = list.collect(Collectors.toList());
            LinkedList<SSTable> ssTables = collect.stream()
                    .map(Path::toFile)
                    .filter(File::isDirectory)
                    .sorted((o1, o2) -> o2.getName().compareTo(o1.getName()))
                    .map(dir -> readSSTable(dir, lsmConf))
                    .collect(Collectors.toCollection(LinkedList::new));
            return new SSTablesManager(tableId, tableDir, lsmConf, ssTables, scheduler);
        }
    }

    private SSTablesManager(String tableId, File tableDir, LsmConf lsmConf, LinkedList<SSTable> ssTables, SimpleScheduler scheduler) {
        this.lsmConf = lsmConf;
        this.ssTablesCnt = new AtomicInteger(ssTables.size());
        this.ssTablesSortedList = new SStablesList(ssTables);
        if (ssTables.size() > 0) {
            this.ssTableLastId = new AtomicInteger(ssTables.get(0).id);
        } else {
            this.ssTableLastId = new AtomicInteger(0);
        }
        this.recordsCnt = new AtomicInteger(ssTables.stream().map(e -> e.ssTableMetadata.records).reduce(0, Integer::sum));
        this.deletedRecordsCnt = new AtomicInteger(ssTables.stream().map(e -> e.ssTableMetadata.deletedRecords).reduce(0, Integer::sum));
        this.tableDir = tableDir;
        this.scheduler = scheduler;
        this.tableId = tableId;
        this.logPrefix = "[ tableId: " + tableId + " ] ";

        if (!lsmConf.enableMergeIfMaxSSTables)
            log.info(logPrefix + "enableMergeIfMaxSSTables is disabled. Number of SSTables can grow endlessly");
        startBackgroundThreads();
    }

    private void startBackgroundThreads() {
        if (lsmConf.enableBackgroundMerge) {
            optimizeRunnable = scheduler.scheduleWithFixedDelaySecNormalPriority(
                    () -> {
                        try {
                            log.info(logPrefix + " merging 2 last ssTables");
                            this.mergeNLastSSTables(2);
                            log.info(logPrefix + " merged 2 last ssTables");
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    },
                    lsmConf.backgroundMergeInitDelay,
                    lsmConf.triggerBackgroundMergeAfterSec
            );
            log.info(logPrefix + "Starting Background merge. Init Delay: "
                    + lsmConf.backgroundMergeInitDelay + " Running each: "
                    + lsmConf.triggerBackgroundMergeAfterSec + " sec");
        } else {
            log.info(logPrefix + " Background merge is disabled");
        }
    }

    public void createSSTableOnDisk(Iterator<KVRecord> recordsIter, int approximateKeyCount) throws IOException {
        File newPartitionDir = createNextSStablePartitionDirectory();
        SSTableWriter ssTableCreator = new SSTableWriter(newPartitionDir, lsmConf, approximateKeyCount);
        ssTableCreator.createSSTable(recordsIter);
        SSTable ssTable = readSSTable(newPartitionDir, lsmConf);
        ssTablesSortedList.addNodeFront(ssTable);
        recordsCnt.addAndGet(ssTable.ssTableMetadata.records);
        deletedRecordsCnt.addAndGet(ssTable.ssTableMetadata.deletedRecords);

        if (lsmConf.enableMergeIfMaxSSTables && ssTablesCnt.get() > lsmConf.maxSSTables) {
            scheduler.execute(Scheduler.Priority.NORMAL, new SSTablesMergerRunnable(this));
        }
    }

    public int getSStableCnt() {
        return ssTablesCnt.get();
    }

    public void mergeNLastSSTables(int number) throws IOException {
        try {
            ssTablesTailMergeLock.lock();
            log.info("merging...");
            if (ssTablesCnt.get() == 1) return;
            if (number < 2) throw new RuntimeException("Cannot merge " + number + " together");

            LinkedList<SStableNode> tablesToMerge = new LinkedList<>();
            LinkedList<SsTableRangeIterator> recordsToMerge = new LinkedList<>();


            SStableNode newTail;

            int nodesToRemove = 0;
            int recCnt = 0;
            {
                SStableNode prevFromTail = ssTablesSortedList.getTail();
                do {
                    tablesToMerge.addFirst(prevFromTail);
                    SSTable ssTable = prevFromTail.ssTable;
                    recordsToMerge.addFirst(ssTable.readAllRecords());
                    recCnt += ssTable.ssTableMetadata.records;
                    nodesToRemove++;
                    newTail = prevFromTail;
                    prevFromTail = prevFromTail.prev;
                } while (nodesToRemove != number && prevFromTail.prev != null);
            }
            String tablesToMergeStr = Arrays.toString(tablesToMerge.stream().map(e -> e.ssTable.id).toArray());
            log.info("Merging tables: " + tablesToMergeStr);


            File newVersionDir = new File(tableDir, newTail.ssTable.id + "v" + (newTail.ssTable.version + 1));
            Files.createDirectory(newVersionDir.toPath());


            LsmRecordsIterator recordsIter = new LsmRecordsIterator(recordsToMerge);
            new SSTableWriter(newVersionDir, lsmConf, recCnt).createSSTable(recordsIter);

            ssTablesSortedList.replaceAndSetAsNewTail(newTail.ssTable, readSSTable(newVersionDir, lsmConf));


            for (SStableNode sStableNode : tablesToMerge) {
                sStableNode.lockWrite();
                sStableNode.ssTable.close();
                FileUtils.deleteDirectory(sStableNode.ssTable.ssTableDirPath);
            }
            for (int i = 0; i < nodesToRemove - 1; i++) {
                ssTablesCnt.decrementAndGet();
            }


            log.info("Merged tables: " + tablesToMergeStr);
        } catch (Throwable e) {
            throw e;

        } finally {
            ssTablesTailMergeLock.unlock();
        }
    }

    public void mergeAllTables() throws IOException {
        mergeNLastSSTables(ssTablesCnt.get());
    }

    private File createNextSStablePartitionDirectory() throws IOException {
        ssTableLastId.incrementAndGet();
        ssTablesCnt.incrementAndGet();
        String dirNameStr = ssTableLastId + "v" + 1;
        File dirPath = new File(tableDir, dirNameStr);
        Files.createDirectory(dirPath.toPath());
        return dirPath;
    }

    public static SSTable readSSTable(File ssTableDirPath, LsmConf lsmConf) { // TODO make private
        try (MetadataJsonRW metadataJsonRW = new MetadataJsonRW(ssTableDirPath);
             BloomFilterRafRW bloomFilterRafRW = new BloomFilterRafRW(ssTableDirPath)) {
            SSTableReader ssTableReader = new SSTableReader(ssTableDirPath, lsmConf.sparseIndexSize);
            SSTableMetadata ssTableMetadata = metadataJsonRW.read();
            BloomFilter bloomFilter = bloomFilterRafRW.read();
            String[] split = ssTableDirPath.getName().split("v");
            int ssTableId = Integer.parseInt(split[0]);
            int ssTableVersion = Integer.parseInt(split[1]);
            return new SSTable(ssTableId, ssTableVersion, ssTableDirPath, ssTableReader, ssTableMetadata, bloomFilter);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void close() throws IOException {
        try {
            if (optimizeRunnable != null) optimizeRunnable.cancel(false);
            ssTablesTailMergeLock.lock();
            ssTablesCnt.set(0);
            ssTablesSortedList.clear();
            for (SSTable ssTable : ssTablesSortedList) {
                ssTable.close();
            }
            ssTablesTailMergeLock.unlock();
        } finally {
        }
    }

    static class SSTablesMergerRunnable implements Runnable {

        private final SSTablesManager ssTablesManager;

        public SSTablesMergerRunnable(SSTablesManager ssTablesManager) {
            this.ssTablesManager = ssTablesManager;
        }

        @Override
        public void run() {
            try {
                log.info("Trying to merge tables");
                ssTablesManager.mergeNLastSSTables(2);
            } catch (Throwable e) {
                log.error("Error");
                System.err.println(e);
                System.err.println(Arrays.toString(e.getStackTrace()));
                throw new RuntimeException("Threade: " + Thread.currentThread().getId() + "\n" + e);
            }
        }

    }

    public static void printThread(String msg) {
        System.out.println("Thread[" + Thread.currentThread().getId() + "]  " + msg);
    }

    public KVRecord searchKey(String key) throws IOException {
        return iterateOverSSTables(ssTableIterator -> {
                    while (ssTableIterator.hasNext()) {
                        KVRecord value = ssTableIterator.next().searchKey(key);
                        if (value != null) return value;
                    }
                    return null;
                });
    }

    public ArrayList<SsTableRangeIterator> searchRange(RangeKey left, RangeKey right) throws IOException {
        ArrayList<SsTableRangeIterator> iterators = new ArrayList<>(ssTablesCnt.get());
        iterateOverSSTables(ssTableIterator -> {
                    while (ssTableIterator.hasNext()) {
                        SSTable ssTable = ssTableIterator.next();
                        iterators.add(ssTable.searchRange(left, right));
                    }
                    return "";
                }
        );
        return iterators;
    }

    public Integer countNotDeletedTotalRec() throws IOException {
        return iterateOverSSTables(ssTableIterator -> {
                    int cnt = 0;
                    while (ssTableIterator.hasNext()) {
                        SSTable ssTable = ssTableIterator.next();
                        cnt += ssTable.ssTableMetadata.numberOfRecordsWithDeleted - ssTable.ssTableMetadata.deletedRecords;
                    }
                    return cnt;
                });
    }

    public ArrayList<Iterator<KVRecord>> getAllRecords() throws IOException {
        ArrayList<Iterator<KVRecord>> iterators = new ArrayList<>(ssTablesCnt.get());
        iterateOverSSTables(ssTableIterator -> {
                    while (ssTableIterator.hasNext()) {
                        SSTable ssTable = ssTableIterator.next();
                        iterators.add(ssTable.readAllRecords());
                    }
                    return "";
                }
        );
        return iterators;
    }

    private <T> T iterateOverSSTables(UnCheckedFunction<Iterator<SSTable>, T, IOException> func) throws IOException {
        SStableNode headElem = ssTablesSortedList.getHead();

        Stack<SStableNode> reentrantLocks = new Stack<>();
        Iterator<SSTable> sstablesIter = new Iterator<>() {

            SStableNode elem = headElem;
            int cnt = 0;

            @Override
            public boolean hasNext() {
                boolean has = elem != null;
                return has;
            }

            @Override
            public SSTable next() {
                elem.lockRead();
                reentrantLocks.push(elem);

                SSTable ssTable = elem.ssTable;

                elem = elem.next;
                cnt++;
                return ssTable;
            }

        };

        try {
            return func.apply(sstablesIter);
        } finally {
            while (!reentrantLocks.empty()) {
                reentrantLocks.pop().unlockRead();
            }
        }

    }

}
