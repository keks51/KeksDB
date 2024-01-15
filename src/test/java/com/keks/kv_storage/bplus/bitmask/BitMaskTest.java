package com.keks.kv_storage.bplus.bitmask;

//import com.keks.kv_storage.bplus.page_manager.page.fixed.BitMaskPage;
//import org.junit.jupiter.api.RepeatedTest;
//import org.junit.jupiter.api.Test;
//
//import java.util.ArrayList;
//import java.util.Collections;
//import java.util.HashSet;
//import java.util.List;
//import java.util.concurrent.*;
//import java.util.concurrent.atomic.AtomicInteger;
//import java.util.concurrent.atomic.AtomicLong;
//import java.util.function.Function;
//import java.util.stream.Collectors;
//import java.util.stream.Stream;
//
//import static org.junit.jupiter.api.Assertions.*;

class BitMaskTest {

//    @Test
//    public void test5() {
//        int bucketsNum = 3;
//        int totalNumberIfBits = bucketsNum * BitMaskOld.BITS_IN_LONG;
//        BitMaskOld bitMask = new BitMaskOld(bucketsNum, 1);
//        for (int i = 0; i < totalNumberIfBits; i++) {
//            bitMask.tryToSet(i);
//        }
//
//        assertEquals(BitMaskOld.MASK_IS_FULL_LONG_VALUE, bitMask.getMasks()[0]);
//        assertEquals(BitMaskOld.MASK_IS_FULL_LONG_VALUE, bitMask.getMasks()[1]);
//        assertEquals(BitMaskOld.MASK_IS_FULL_LONG_VALUE, bitMask.getMasks()[2]);
//        assertEquals(totalNumberIfBits, bitMask.getSetBitsCnt());
//
//        for (int i = 0; i < totalNumberIfBits; i++) {
//            assertTrue(bitMask.isSetLockRead(i));
//        }
//
//        for (int i = 0; i < totalNumberIfBits; i++) {
//            bitMask.unSetBitLockWrite(i);
//        }
//
//        assertEquals(0L, bitMask.getMasks()[0]);
//        assertEquals(0L, bitMask.getMasks()[1]);
//        assertEquals(0L, bitMask.getMasks()[2]);
//        assertEquals(0, bitMask.getSetBitsCnt());
//
//        for (int i = 0; i < totalNumberIfBits; i++) {
//            assertFalse(bitMask.isSetLockRead(i));
//        }
//    }
//
//    @Test
//    public void test6() {
//        int bucketsNum = 3;
//        int totalNumberIfBits = bucketsNum * BitMaskOld.BITS_IN_LONG;
//        BitMaskOld bitMask = new BitMaskOld(bucketsNum, 1);
//        for (int i = 0; i < totalNumberIfBits; i++) {
//            bitMask.unSetBitLockWrite(i);
//        }
//
//        assertEquals(0L, bitMask.getMasks()[0]);
//        assertEquals(0L, bitMask.getMasks()[1]);
//        assertEquals(0L, bitMask.getMasks()[2]);
//        assertEquals(0, bitMask.getSetBitsCnt());
//
//        for (int i = 0; i < totalNumberIfBits; i++) {
//            assertFalse(bitMask.isSetLockRead(i));
//        }
//
//        for (int i = 0; i < totalNumberIfBits; i++) {
//            bitMask.tryToSet(i);
//        }
//
//        assertEquals(BitMaskOld.MASK_IS_FULL_LONG_VALUE, bitMask.getMasks()[0]);
//        assertEquals(BitMaskOld.MASK_IS_FULL_LONG_VALUE, bitMask.getMasks()[1]);
//        assertEquals(BitMaskOld.MASK_IS_FULL_LONG_VALUE, bitMask.getMasks()[2]);
//        assertEquals(totalNumberIfBits, bitMask.getSetBitsCnt());
//
//        for (int i = 0; i < totalNumberIfBits; i++) {
//            bitMask.unSetBitLockWrite(i);
//        }
//
//        assertEquals(0L, bitMask.getMasks()[0]);
//        assertEquals(0L, bitMask.getMasks()[1]);
//        assertEquals(0L, bitMask.getMasks()[2]);
//        assertEquals(0, bitMask.getSetBitsCnt());
//
//        for (int i = 0; i < totalNumberIfBits; i++) {
//            assertFalse(bitMask.isSetLockRead(i));
//        }
//    }
//
//    @Test
//    public void test7() {
//        BitMaskOld bitMask = new BitMaskOld(3, 1);
//        assertEquals(0, bitMask.nextUnsetBit(new ConcurrentSkipListMap<>(), new AtomicLong()));
//
//        bitMask.tryToSet(0);
//        bitMask.tryToSet(1);
//        bitMask.tryToSet(3);
//        assertEquals(2, bitMask.nextUnsetBit(new ConcurrentSkipListMap<>(), new AtomicLong()));
//        assertEquals(4, bitMask.getSetBitsCnt());
//
//        for (int i = 0; i < BitMaskOld.BITS_IN_LONG; i++) {
//            bitMask.tryToSet(i);
//        }
//        assertEquals(BitMaskOld.BITS_IN_LONG, bitMask.getSetBitsCnt());
//        assertEquals(BitMaskOld.BITS_IN_LONG, bitMask.nextUnsetBit(new ConcurrentSkipListMap<>(), new AtomicLong()));
//
//
//        for (int i = BitMaskOld.BITS_IN_LONG; i < 2 * BitMaskOld.BITS_IN_LONG; i++) {
//            bitMask.tryToSet(i);
//        }
//        assertEquals(2 * BitMaskOld.BITS_IN_LONG, bitMask.getSetBitsCnt());
//        assertEquals(2 * BitMaskOld.BITS_IN_LONG, bitMask.nextUnsetBit(new ConcurrentSkipListMap<>(), new AtomicLong()));
//
//
//        for (int i = 2 * BitMaskOld.BITS_IN_LONG; i < 3 * BitMaskOld.BITS_IN_LONG; i++) {
//            bitMask.tryToSet(i);
//        }
//        assertEquals(-1, bitMask.nextUnsetBit(new ConcurrentSkipListMap<>(), new AtomicLong()));
//        assertEquals(3 * BitMaskOld.BITS_IN_LONG, bitMask.getSetBitsCnt());
//    }
//
//    @Test
//    public void test8() throws ExecutionException, InterruptedException, TimeoutException {
//        int bucketsNum = 3;
//        BitMaskOld bitMask = new BitMaskOld(bucketsNum, 1);
//        int totalNumberIfBits = bucketsNum * BitMaskOld.BITS_IN_LONG;
//
//        Function<Integer, String> func1 = bit -> {
//            bitMask.tryToSet(bit);
//            return "";
//        };
//        runConcurrentTest(totalNumberIfBits, 1000, 1, func1, 30);
//
//        assertEquals(BitMaskOld.MASK_IS_FULL_LONG_VALUE, bitMask.getMasks()[0]);
//        assertEquals(BitMaskOld.MASK_IS_FULL_LONG_VALUE, bitMask.getMasks()[1]);
//        assertEquals(BitMaskOld.MASK_IS_FULL_LONG_VALUE, bitMask.getMasks()[2]);
//        assertEquals(totalNumberIfBits, bitMask.getSetBitsCnt());
//
//        Function<Integer, String> func2 = bit -> {
//            bitMask.unSetBitLockWrite(bit);
//            return "";
//        };
//        runConcurrentTest(totalNumberIfBits, 1000, 1, func2, 30);
//
//        assertEquals(BitMaskOld.MASK_IS_EMPTY_LONG_VALUE, bitMask.getMasks()[0]);
//        assertEquals(BitMaskOld.MASK_IS_EMPTY_LONG_VALUE, bitMask.getMasks()[1]);
//        assertEquals(BitMaskOld.MASK_IS_EMPTY_LONG_VALUE, bitMask.getMasks()[2]);
//        assertEquals(0, bitMask.getSetBitsCnt());
//    }
//
//    @RepeatedTest(10)
//    public void test9() throws ExecutionException, InterruptedException, TimeoutException {
//        int bucketsNum = 1022;
//        BitMaskOld bitMask = new BitMaskOld(bucketsNum, 4);
//        int totalNumberIfBits = bucketsNum * BitMaskOld.BITS_IN_LONG;
//
//        List<Integer> setBits = Collections.synchronizedList(new ArrayList<>());
//        AtomicInteger notSetCnt = new AtomicInteger(0);
//        Function<Integer, String> func1 = bit -> {
//            int i = bitMask.nextUnsetBit(new ConcurrentSkipListMap<>(), new AtomicLong());
//            if (i == -1) {
//                notSetCnt.incrementAndGet();
//            } else {
//                setBits.add(i);
//            }
//            return "";
//        };
//        runConcurrentTest(totalNumberIfBits, 1000, 1, func1, 100);
//
//        assertEquals(BitMaskOld.MASK_IS_FULL_LONG_VALUE, bitMask.getMasks()[0]);
//        assertEquals(BitMaskOld.MASK_IS_FULL_LONG_VALUE, bitMask.getMasks()[1]);
//        assertEquals(BitMaskOld.MASK_IS_FULL_LONG_VALUE, bitMask.getMasks()[2]);
//        assertEquals(totalNumberIfBits, bitMask.getSetBitsCnt() + notSetCnt.get());
//        System.out.println(setBits.size());
//        System.out.println(new HashSet<>(setBits).size());
//        assertEquals(bucketsNum * BitMaskOld.BITS_IN_LONG, setBits.size() + notSetCnt.get());
//        assertEquals(bucketsNum * BitMaskOld.BITS_IN_LONG, new HashSet<>(setBits).size() + notSetCnt.get());
//    }
//
//    @Test
//    public void test10() {
//        int buckets = 64;
//        for (int nextBucketStep = 1; nextBucketStep < buckets; nextBucketStep++) {
//            BitMaskOld bitMask = new BitMaskOld(buckets, nextBucketStep);
//            ArrayList<Integer> list = new ArrayList<>();
//            for (int pos = 0; pos < buckets * BitMaskOld.BITS_IN_LONG; pos++) {
//                int i = bitMask.nextUnsetBit(new ConcurrentSkipListMap<>(), new AtomicLong());
//                list.add(i);
//            }
//            HashSet<Integer> integers = new HashSet<>(list);
//            assertEquals(list.size(), integers.size());
//            assertTrue(bitMask.isFull());
//        }
//    }
//
//    @Test
//    public void test11() throws ExecutionException, InterruptedException, TimeoutException {
//        int bucketsNum = 1022;
//        BitMaskOld bitMask = new BitMaskOld(bucketsNum, 4);
//        int totalNumberIfBits = bucketsNum * BitMaskOld.BITS_IN_LONG;
//        List<Integer> list1 = Collections.synchronizedList(new ArrayList<>(totalNumberIfBits));
//        List<Integer> list2 = Collections.synchronizedList(new ArrayList<>(totalNumberIfBits));
//        List<Integer> bits = Collections.synchronizedList(new ArrayList<>());
//        Function<Integer, String> func1 = bit -> {
//            if (bitMask.tryToSet(bit)) {
//                list1.add(bit);
//            }
//            int i = bitMask.nextUnsetBit(new ConcurrentSkipListMap<>(), new AtomicLong());
//
//            if (i == -1) {
//                System.out.println("here");
//            } else {
//                list2.add(i);
//            }
//            bits.add(i);
//            return "";
//        };
//        runConcurrentTest(totalNumberIfBits, 1000, 1, func1, 100);
//        System.out.println();
//        System.out.println("TotalBits: " + totalNumberIfBits);
//        System.out.println("TryToSet: " + list1.size());
//        System.out.println("NextUnset: " + list2.size());
//        List<Integer> sum = Stream.concat(list1.stream(), list2.stream()).collect(Collectors.toList());
//        assertEquals(totalNumberIfBits, sum.size());
//        System.out.println("Sum: " + sum.size());
//        System.out.println("Unique: " + new HashSet<>(sum).size());
//        assertEquals(totalNumberIfBits, new HashSet<>(sum).size());
//        assertEquals(totalNumberIfBits, bitMask.getSetBitsCnt());
//
//        for (int bucketNum = 0; bucketNum < bucketsNum; bucketNum++) {
//            assertEquals(BitMaskOld.MASK_IS_FULL_LONG_VALUE, bitMask.getMasks()[bucketNum]);
//        }
//
//    }
//
//
//    public static void runConcurrentTest(int taskCount,
//                                         int threadPoolAwaitTimeoutSec,
//                                         int taskAwaitTimeoutSec,
//                                         Function<Integer, String> function,
//                                         int numberOfThreads) throws InterruptedException, ExecutionException, TimeoutException {
//        ExecutorService executor = Executors.newFixedThreadPool(numberOfThreads);
//        List<Future<?>> futures = new ArrayList<>(taskCount);
//        for (int i = 0; i < taskCount; i++) {
//            int y = i;
//            Future<?> future1 = executor.submit(() -> function.apply(y));
//            futures.add(future1);
//        }
//
//        executor.shutdown();
//        if (!executor.awaitTermination(threadPoolAwaitTimeoutSec, TimeUnit.SECONDS)) {
//            throw new InterruptedException();
//        }
//
//        for (Future<?> future : futures) {
//            future.get(taskAwaitTimeoutSec, TimeUnit.SECONDS);
//        }
//
//    }
//
//
//


}