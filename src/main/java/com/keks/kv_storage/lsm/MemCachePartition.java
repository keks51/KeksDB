package com.keks.kv_storage.lsm;

import com.keks.kv_storage.record.KVRecord;

import java.util.Iterator;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;


public class MemCachePartition {


    public final ReentrantReadWriteLock updateLock = new ReentrantReadWriteLock();

    public final long id;

    private final SortedMap<String, KVRecord> kvInMemory = new ConcurrentSkipListMap<>();

    private final AtomicInteger recordsCnt = new AtomicInteger(0);
    private final AtomicInteger deletedRecCnt = new AtomicInteger(0);

    public final int size;

    private volatile boolean isFlushed;

    public MemCachePartition(int size, long id) {
        this.size = size;
        this.id = id;
    }

    public boolean isEmpty() {
        return kvInMemory.isEmpty();
    }

    public Iterator<KVRecord> getIter() {
        return kvInMemory.values().iterator();
    }

    public int getRecordsCnt() {
        return recordsCnt.get();
    }

    public int getDelRecordsCnt() {
        return deletedRecCnt.get();
    }

    public KVRecord get(String key) {
        return kvInMemory.get(key);
    }

    public KVRecord put(String key, KVRecord kvRecord) {
        KVRecord previous = kvInMemory.put(key, kvRecord);
        if (previous == null) recordsCnt.incrementAndGet();
        return previous;
    }

    public Iterator<KVRecord> headSubMap(String right) {
        return kvInMemory.headMap(right).values().iterator();
    }

    public Iterator<KVRecord> tailSubMap(String left) {
        return kvInMemory.tailMap(left).values().iterator();
    }

    public Iterator<KVRecord> subMap(String left, String right) {
        return kvInMemory.subMap(left, right).values().iterator();
    }

    public void setIsFlushed() {
        try {
            isFlushed = true;
        } finally {
        }

    }

    public boolean isFlushed() {
        return isFlushed;
    }

    public void lockRead() {
        updateLock.readLock().lock();
    }

    public void unLockRead() {
        updateLock.readLock().unlock();
    }

    public void lockWrite() {
        updateLock.writeLock().lock();
    }

    public void unLockWrite() {
        updateLock.writeLock().unlock();
    }


}
