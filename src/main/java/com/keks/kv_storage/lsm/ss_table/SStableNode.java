package com.keks.kv_storage.lsm.ss_table;

import java.util.concurrent.locks.ReentrantReadWriteLock;

public class SStableNode {

    // stores data
    private final ReentrantReadWriteLock lock;
    public volatile SSTable ssTable;

    // pointer to the next node
    public volatile SStableNode next;

    // pointer to the previous node
    public volatile SStableNode prev;

    public SStableNode(SSTable ssTable, SStableNode next, SStableNode prev) {
        this.ssTable = ssTable;
        this.next = next;
        this.prev = prev;
        this.lock = new ReentrantReadWriteLock();
    }

    public void lockRead() {
        lock.readLock().lock();
    }

    public void unlockRead() {
        lock.readLock().unlock();
    }

    public void lockWrite() {
        lock.writeLock().lock();
    }

    public void unlockWrite() {
        lock.writeLock().unlock();
    }

}
