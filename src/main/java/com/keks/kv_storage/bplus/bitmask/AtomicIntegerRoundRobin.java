package com.keks.kv_storage.bplus.bitmask;

import java.util.concurrent.atomic.AtomicInteger;


public class AtomicIntegerRoundRobin {

    private final int totalIndexes;
    private final AtomicInteger atomicInteger = new AtomicInteger(-1);

    public AtomicIntegerRoundRobin(int totalIndexes) {
        this.totalIndexes = totalIndexes;
    }

    public int get() {
        return atomicInteger.get() % totalIndexes;
    }

    public int next() {
        int currentIndex;
        int nextIndex;

        do {
            currentIndex = atomicInteger.get();
            nextIndex = currentIndex < Integer.MAX_VALUE ? currentIndex + 1 : 0;
        } while (!atomicInteger.compareAndSet(currentIndex, nextIndex));

        return nextIndex % totalIndexes;
    }

    public int showNext() {
        return (atomicInteger.get() + 1) % totalIndexes;
    }

    public int showPrev() {
        return (atomicInteger.get() - 1) % totalIndexes;
    }

    public void reset() {
        int currentIndex;
        do {
            currentIndex = atomicInteger.get();
        } while (!atomicInteger.compareAndSet(currentIndex, -1));
    }

    public boolean tryReset() {
        boolean b = atomicInteger.compareAndSet(atomicInteger.get(), -1);
        return b;
    }

}
