package com.keks.kv_storage.lsm;

import com.keks.kv_storage.record.KVRecord;
import com.keks.kv_storage.query.range.MaxRangeKey;
import com.keks.kv_storage.query.range.MinRangeKey;
import com.keks.kv_storage.query.range.RangeKey;
import com.keks.kv_storage.query.range.RangeSearchKey;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;


public class MemCacheTable {

    static final Logger log = LogManager.getLogger(MemCacheTable.class.getName());

    private final ConcurrentLinkedDeque<MemCachePartition> partitions = new ConcurrentLinkedDeque<>();
    private final AtomicLong ids = new AtomicLong();

    private final int maxPartitionSize;

    public MemCacheTable(int maxPartitionSize) {
        this.maxPartitionSize = maxPartitionSize;
        this.partitions.add(new MemCachePartition(maxPartitionSize, ids.getAndIncrement()));
    }

    public boolean isEmpty() {
        assert partitions.peek() != null;
        return partitions.peek().isEmpty();
    }

    public void addNewPartition() {
        partitions.offer(new MemCachePartition(maxPartitionSize, ids.getAndIncrement()));
    }

    public void setOldestIsFlushed() {
        MemCachePartition first = partitions.getFirst();
        try {
            first.lockWrite();
            first.setIsFlushed();
            log.info(first.id + " mem_cache partition was flushed");
        } finally {
            first.unLockWrite();
        }
    }

    public Iterator<KVRecord> getOldestPartitionIter() {
        return partitions.getFirst().getIter();
    }

    // for tests
    protected MemCachePartition getOldestPartition() {
        return partitions.getFirst();
    }

    // for tests
    protected MemCachePartition getNewestPartition() {
        return partitions.getLast();
    }

    public void dropOldestPartition() {
        partitions.removeFirst();
    }

    public KVRecord get(String key) {
        Iterator<MemCachePartition> iterator = partitions.descendingIterator();
        while (iterator.hasNext()) {
            KVRecord kvRecord = iterator.next().get(key);
            if (kvRecord != null) return kvRecord;
        }
        return null;
    }

    public void remove(String key) {
        update(key, new KVRecord(key));
    }

    protected void put(String key, String value) {
        put(key, new KVRecord(key, value));
    }

    public void put(String key, KVRecord record) {
        update(key, record);
    }

    private void update(String key, KVRecord record) {
        boolean isFlushed;
        do {
            MemCachePartition last = partitions.getLast();
            try {
                last.lockRead();
                isFlushed = last.isFlushed();
                if (!isFlushed) {
                    last.put(key, record);
                }
            } finally {
                last.unLockRead();
            }
        } while (isFlushed);


    }

    public int getApproxRecCnt() {
        return partitions.getLast().getRecordsCnt();
    }

    public int getPartitionsCount() {
        return partitions.size();
    }

    public boolean isFull() {
        return maxPartitionSize <= partitions.getLast().getRecordsCnt();
    }

    protected List<Iterator<KVRecord>> getRangeFromParts(String left) {
        return iteratePartitions(partition -> partition.tailSubMap(left));
    }

    protected List<Iterator<KVRecord>> getRangeToParts(String right) {
        return iteratePartitions(partition -> partition.headSubMap(right +"\0")); // +"\0" to make inclusive for strings
    }

    protected List<Iterator<KVRecord>> getRangeParts(String left, String right) {
        return iteratePartitions(partition -> partition.subMap(left, right +"\0")); // +"\0" to make inclusive for strings
    }

    protected List<Iterator<KVRecord>> getAllParts(RangeKey left, RangeKey right) {
        if (left instanceof MinRangeKey && right instanceof MaxRangeKey) {
            return getRangeFromParts("");
        } else  if (left instanceof MinRangeKey && right instanceof RangeSearchKey) {
            return getRangeToParts(((RangeSearchKey) right).key);
        } else  if (left instanceof RangeSearchKey && right instanceof MaxRangeKey) {
            return getRangeFromParts(((RangeSearchKey) left).key);
        } else {
            return getRangeParts(((RangeSearchKey) left).key, ((RangeSearchKey) right).key);
        }

    }

    private List<Iterator<KVRecord>> iteratePartitions(Function<MemCachePartition, Iterator<KVRecord>> func) {
        LinkedList<Iterator<KVRecord>> res = new LinkedList<>();
        Iterator<MemCachePartition> iterator = partitions.descendingIterator();
        while (iterator.hasNext()) {
            MemCachePartition partition = iterator.next();
            res.addLast(func.apply(partition));
        }
        return res;
    }

}
