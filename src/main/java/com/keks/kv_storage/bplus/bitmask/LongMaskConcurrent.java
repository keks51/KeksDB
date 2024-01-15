package com.keks.kv_storage.bplus.bitmask;

import java.util.concurrent.locks.ReentrantReadWriteLock;


public class LongMaskConcurrent extends LongMask {


    public final ReentrantReadWriteLock lock;
    public final long maskId;

    public LongMaskConcurrent(long maskId) {
        super(0L);
        this.maskId = maskId;
        this.lock = new ReentrantReadWriteLock();
    }

    public LongMaskConcurrent(long maskValue, long maskId) {
        super(maskValue);
        this.maskId = maskId;
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

    public boolean tryLockWrite() {
        return lock.writeLock().tryLock();
    }

    public boolean tryLockRead() {
        return lock.readLock().tryLock();
    }

    public void unlockWrite() {
        lock.writeLock().unlock();
    }

    @Override
    public boolean isFull() {
        try {
            lockRead();
            return mask == MASK_IS_FULL_LONG_VALUE;
        } finally {
            unlockRead();
        }
    }

    @Override
    public int getFirstUnsetFromRight() {
      try {
          lockWrite();
          return super.getFirstUnsetFromRight();
      } finally {
          unlockWrite();
      }
    }

    public boolean isSet(long pos) {
        try {
            lockRead();
          return super.isSet(pos);
        } finally {
            unlockRead();
        }
    }

    public void set(long pos) {
        try {
            lockWrite();
            super.set(pos);
        } finally {
            unlockWrite();
        }
    }

    public boolean tryToSet(long pos) {
        if (tryLockWrite()) {
            try {
                if (!isSet(pos)) {
                    set(pos);
                    return true;
                } else {
                    return false;
                }
            } finally {
                unlockWrite();
            }
        } else {
            return false;
        }
    }

    public boolean unSet(long pos) {
        try {
            lockWrite();
            return super.unSet(pos);
        } finally {
            unlockWrite();
        }
    }

    public void clear() {
        try {
            lockWrite();
            super.clear();
        } finally {
            unlockWrite();
        }
    }

    @Override
    public String toString() {
        try {
            lockRead();
            return "MaskId: " + maskId + " " + String.format("%64s", Long.toBinaryString(mask)).replace(' ', '0');
        } finally {
            unlockRead();
        }
    }
}
