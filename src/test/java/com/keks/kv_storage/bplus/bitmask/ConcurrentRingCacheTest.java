package com.keks.kv_storage.bplus.bitmask;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;


class ConcurrentRingCacheTest {

    @Test
    public void test1() {

        ConcurrentRingCache<Integer> ringCache = new ConcurrentRingCache<>(5);

//        System.out.println(ringCache.getCurrentSize());
        ringCache.put(1);
        ringCache.put(2);
        ringCache.put(3);
        ringCache.put(4);
        ringCache.put(5);
        assert (5 == ringCache.getCurrentSize());
        assert (0 == ringCache.getHeadPos());
        assert (4 == ringCache.getTailPos());
        assert (1 == ringCache.getHeadElem());
        assert (5 == ringCache.getTailElem());

        ringCache.put(6);
        ringCache.put(7);
        ringCache.put(8);
        ringCache.put(9);
        ringCache.put(10);
        assert (5 == ringCache.getCurrentSize());
        assert (0 == ringCache.getHeadPos());
        assert (4 == ringCache.getTailPos());
        assert (6 == ringCache.getHeadElem());
        assert (10 == ringCache.getTailElem());

        ringCache.dropOldest();
        ringCache.dropOldest();
        ringCache.dropOldest();
        ringCache.dropOldest();
        assert (1 == ringCache.getCurrentSize());
        assert (4 == ringCache.getHeadPos());
        assert (4 == ringCache.getTailPos());
        assert (10 == ringCache.getHeadElem());
        assert (10 == ringCache.getTailElem());


        ringCache.dropOldest();
        assert (0 == ringCache.getCurrentSize());
        assert (-1 == ringCache.getHeadPos());
        assert (-1 == ringCache.getTailPos());
        assert (null == ringCache.getHeadElem());
        assert (null == ringCache.getTailElem());

//        System.out.println("Size: " + ringCache.getCurrentSize());
//        System.out.println("HeadPos: " + ringCache.getHeadPos() + " HeadElem: " + ringCache.getHeadElem());
//        System.out.println("TailPos: " + ringCache.getTailPos() + " TailElem: " + ringCache.getTailElem());

//        assert (0 == ringCache.getCurrentSize());
//        assert (-1 == ringCache.getHeadPos());
//        assert (-1 == ringCache.getTailPos());
//        assert (null == ringCache.getHeadElem());
//        assert (null == ringCache.getTailElem());



    }

    @Test
    public void test2() throws ExecutionException, InterruptedException, TimeoutException {
        ConcurrentRingCache<Integer> ringCache = new ConcurrentRingCache<>(5);

        AtomicInteger atomicInteger = new AtomicInteger();
        Function<Integer, String> func = i -> {
            int a = atomicInteger.getAndIncrement();
            ringCache.put(a);
            if (a % 2 == 0) ringCache.dropOldest();
            return "";
        };

        runConcurrentTest(100, 10_000, 10, func, 100);

        System.out.println(ringCache);
        System.out.println("Size: " + ringCache.getCurrentSize());
        System.out.println("HeadPos: " + ringCache.getHeadPos() + " HeadElem: " + ringCache.getHeadElem());
        System.out.println("TailPos: " + ringCache.getTailPos() + " TailElem: " + ringCache.getTailElem());

    }

    @Test
    public void test3() throws ExecutionException, InterruptedException, TimeoutException {
        ConcurrentRingCache<Integer> ringCache = new ConcurrentRingCache<>(5);

        AtomicInteger atomicInteger = new AtomicInteger();
        Function<Integer, String> func = i -> {
            int a = atomicInteger.getAndIncrement();
            if (a % 2 == 0)  {
                ringCache.put(a);
                if (!ringCache.isEmpty()) System.out.println(ringCache);
            }
            ringCache.dropOldest();

            return "";
        };

        runConcurrentTest(100, 10_000, 10, func, 100);

        System.out.println(ringCache);
        System.out.println("Size: " + ringCache.getCurrentSize());
        System.out.println("HeadPos: " + ringCache.getHeadPos() + " HeadElem: " + ringCache.getHeadElem());
        System.out.println("TailPos: " + ringCache.getTailPos() + " TailElem: " + ringCache.getTailElem());

    }

    public static void runConcurrentTest(int taskCount,
                                         int threadPoolAwaitTimeoutSec,
                                         int taskAwaitTimeoutSec,
                                         Function<Integer, String> function,
                                         int numberOfThreads) throws InterruptedException, ExecutionException, TimeoutException {
        ExecutorService executor = Executors.newFixedThreadPool(numberOfThreads);
        List<Future<?>> futures = new ArrayList<>(taskCount);
        for (int i = 0; i < taskCount; i++) {
            int y = i;
            Future<?> future1 = executor.submit(() -> function.apply(y));
            futures.add(future1);
        }

        executor.shutdown();
        if (!executor.awaitTermination(threadPoolAwaitTimeoutSec, TimeUnit.SECONDS)) {
            throw new InterruptedException();
        }

        for (Future<?> future : futures) {
            future.get(taskAwaitTimeoutSec, TimeUnit.SECONDS);
//            future.get();
        }

    }

}