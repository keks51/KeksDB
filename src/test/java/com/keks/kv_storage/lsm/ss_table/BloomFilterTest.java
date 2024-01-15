package com.keks.kv_storage.lsm.ss_table;

import com.keks.kv_storage.lsm.utils.BloomFilter;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;


class BloomFilterTest {

    @Test
    public void testCalcOptimalNumOfBits() {
        assertEquals(64, BloomFilter.calcOptimalNumOfBits(0, 0.99));
        assertEquals(64, BloomFilter.calcOptimalNumOfBits(1, 0.99));
        assertEquals(64, BloomFilter.calcOptimalNumOfBits(2, 0.99));
//
//        System.out.println("0.50000 " + BloomFilter.calcOptimalNumOfBits(2_000_000, 0.05000));
//        System.out.println("0.50000 " + BloomFilter.calcOptimalNumOfBits(2_000_000, 0.10000));
//        System.out.println("0.50000 " + BloomFilter.calcOptimalNumOfBits(2_000_000, 0.20000));
//        System.out.println("0.50000 " + BloomFilter.calcOptimalNumOfBits(2_000_000, 0.30000));
//        System.out.println("0.50000 " + BloomFilter.calcOptimalNumOfBits(2_000_000, 0.40000));
//        System.out.println("0.60000 " + BloomFilter.calcOptimalNumOfBits(2_000_000, 0.60000));
//        System.out.println("0.70000 " + BloomFilter.calcOptimalNumOfBits(2_000_000, 0.70000));
//        System.out.println("0.80000 " + BloomFilter.calcOptimalNumOfBits(2_000_000, 0.80000));
//        System.out.println("0.90000 " + BloomFilter.calcOptimalNumOfBits(2_000_000, 0.90000));
//        System.out.println("0.95000 " + BloomFilter.calcOptimalNumOfBits(2_000_000, 0.95000));
//        System.out.println("1.96000 " + BloomFilter.calcOptimalNumOfBits(2_000_000, 0.96000));
//        System.out.println("1.97000 " + BloomFilter.calcOptimalNumOfBits(2_000_000, 0.97000));
//        System.out.println("1.98000 " + BloomFilter.calcOptimalNumOfBits(2_000_000, 0.98000));
//        System.out.println("1.99000 " + BloomFilter.calcOptimalNumOfBits(2_000_000, 0.99000));
//        System.out.println("1.99500 " + BloomFilter.calcOptimalNumOfBits(2_000_000, 0.99500));
//        System.out.println("1.99900 " + BloomFilter.calcOptimalNumOfBits(2_000_000, 0.99900));
//        System.out.println("1.99950 " + BloomFilter.calcOptimalNumOfBits(2_000_000, 0.99950));
//        System.out.println("1.99990 " + BloomFilter.calcOptimalNumOfBits(2_000_000, 0.99990));
//        System.out.println("1.99995 " + BloomFilter.calcOptimalNumOfBits(2_000_000, 0.99995));
//        System.out.println("1.99999 " + BloomFilter.calcOptimalNumOfBits(2, 0.99999));

    }

}