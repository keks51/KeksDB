package com.keks.kv_storage.bplus.buffer;

import java.util.concurrent.locks.ReentrantReadWriteLock;


public class PagesGroupLock {

    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    boolean tryLockWrite() {
        return lock.writeLock().tryLock();
    }

    void lockWrite() {
        lock.writeLock().lock();
    }

    void unlockWrite() {
        lock.writeLock().unlock();
    }

    void lockRead() {
        lock.readLock().lock();
    }

    void unlockRead() {
        lock.readLock().unlock();
    }

}
