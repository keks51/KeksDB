package com.keks.kv_storage.bplus.bitmask;


public class ConcurrentRingCache<T> {

    public final int maxSize;

    private final AtomicIntegerRoundRobin headPosRing;

    private final AtomicIntegerRoundRobin tailPosRing;

    private Object[] cache;
    private boolean isFull;
    private int elemsInCache;

    public ConcurrentRingCache(int maxSize) {
        this.maxSize = maxSize;
        this.cache = new Object[maxSize];
        this.headPosRing = new AtomicIntegerRoundRobin(maxSize);
        this.tailPosRing = new AtomicIntegerRoundRobin(maxSize);
    }

    public void put(T elem) {
        int nextTailPos = tailPosRing.next();
        if (headPosRing.get() == -1 && nextTailPos == 0) {
            cache[0] = elem;
            headPosRing.next();
        } else {
            if (nextTailPos == headPosRing.get()) headPosRing.next();
            cache[nextTailPos] = elem;
        }

    }

    public int getHeadPos() {
        return headPosRing.get();
    }

    public int getTailPos() {
        return tailPosRing.get();
    }

    public boolean isFull() {
        return (headPosRing.get() == tailPosRing.showNext());
    }

    public boolean isEmpty() {
        return headPosRing.get() == -1 && tailPosRing.get() == -1;
    }

    public int getCurrentSize() {
        if (isEmpty()) return 0;
        int tailPos = tailPosRing.get();
        int headPos = headPosRing.get();
        if (headPos > tailPos) tailPos = tailPos + maxSize;

        return tailPos - headPos + 1;
    }

    public void dropOldest() {
        int pos = headPosRing.get();
        if (pos != -1) {
            headPosRing.next();
            cache[pos] = null;
            if (pos == tailPosRing.get() && tailPosRing.get() == headPosRing.showPrev() && tailPosRing.tryReset()) {
                headPosRing.reset();
            }
        }
    }

    public T getHeadElem() {
        int pos = headPosRing.get();
        if (pos == -1) {
            return null;
        } else {
            return (T) cache[pos];
        }
    }

    public T getTailElem() {
        int pos = tailPosRing.get();
        if (pos == -1) {
            return null;
        } else {
            return (T) cache[pos];
        }
    }

    public T getElem(int pos) {
        if (pos >= maxSize) throw new IndexOutOfBoundsException("Incorrect pos: " + pos + "  maxSize: " + maxSize);
        return (T) cache[pos];
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (Object o : cache) {
            sb.append(o).append("\n");
        }
        return sb.toString();
    }
}
