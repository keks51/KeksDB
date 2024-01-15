package com.keks.kv_storage.lsm.utils;

import java.util.List;
import java.util.function.Function;


public class BloomFilter {

    private final int numberOfTotalBits;
    private final List<Function<String, Integer>> hashFunctions;
    public final long[] longArray;
    public int[] intArray;
    private int insertedElemCount = 0;
    public final double falsePositiveRate;
    public final double memoryConsumptionKBytes;

    public BloomFilter(int numberOfRecords, double falsePositiveRate) {
        this(numberOfRecords, falsePositiveRate, List.of(HashFunctions::md5, HashFunctions::murmur));
    }

    public BloomFilter(long[] longArray, double falsePositiveRate, int insertedElemCount) {
        assert falsePositiveRate > 0;
        this.numberOfTotalBits = longArray.length * 64;
        this.hashFunctions = List.of(HashFunctions::md5, HashFunctions::murmur);
        this.longArray = longArray;
        this.falsePositiveRate = falsePositiveRate;
        this.insertedElemCount = insertedElemCount;
        this.memoryConsumptionKBytes = numberOfTotalBits / 8.0 / 1024;
    }

    public BloomFilter(int numberOfRecords, double falsePositiveRate, List<Function<String, Integer>> hashFunctions) {
        assert numberOfRecords > 0;
        assert falsePositiveRate > 0;
        this.falsePositiveRate = falsePositiveRate;
        this.numberOfTotalBits = calcOptimalNumOfBits(numberOfRecords, falsePositiveRate);
        this.hashFunctions = hashFunctions;
        this.longArray = new long[numberOfTotalBits >>> 6];
        this.intArray = new int[numberOfTotalBits >>> 6];
        this.memoryConsumptionKBytes = numberOfTotalBits / 8.0 / 1024;
//        System.out.println("ArrSize: " + this.longArray.length + " NumberOfBits: " + this.numberOfTotalBits + " Memory consumed KB: " + numberOfTotalBits / 8 / 1024);
    }

    public int getSize() {
        return numberOfTotalBits;
    }

    public int getNumberOfInsertedElems() {
        return insertedElemCount;
    }

    private int getPosOfBitToSet(int hash) {
        return Math.floorMod(hash, numberOfTotalBits);
    }

    public void add(String elem) {
        insertedElemCount += 1;
        hashFunctions.forEach(f -> {
            try {
                int bitPos = getPosOfBitToSet(f.apply(elem));
                int maskPos = bitPos >>> 6; // bitToSetIndex / 2^6 or bitToSetIndex / 64
                long bitMask = (1L << bitPos);
                longArray[maskPos] |= bitMask;
            } catch (ArithmeticException e) {
                e.printStackTrace();
                throw e;
            }

        });
    }

    public boolean contains(String elem) {
        for (Function<String, Integer> func : hashFunctions) {
            int bitPos = getPosOfBitToSet(func.apply(elem));
            if ((longArray[bitPos >>> 6] & (1L << bitPos)) == 0) {
                return false;
            }
        }
        return true;
    }

    public double getFalsePosProb() {
        double e = Math.exp(1);
        double x = (hashFunctions.size() * insertedElemCount) / (double) numberOfTotalBits;
        double y = (1 - Math.pow(e, -(x)));
        return Math.pow(y, hashFunctions.size());
    }

    public static int calcOptimalNumOfBits(int records, double rate) {
        assert rate != 0;
        int optimalNumber = (int) (-records * Math.log(rate) / (Math.log(2) * Math.log(2)));
        int i = ((int) (Math.ceil(optimalNumber / 64.0))) * 64;
        return Math.max(i, 64);
    }

    public static double calcMemoryUsageInKb(int records, double rate) {
        int numberOfTotalBits = calcOptimalNumOfBits(records, rate);
        return numberOfTotalBits / 8.0 / 1024;
    }

    static int optimalNumOfHashFunctions(long n, long m) {
        // (m / n) * log(2), but avoid truncation due to division!
        return Math.max(1, (int) Math.round((double) m / n * Math.log(2)));
    }

    @Override
    public String toString() {
        return "BloomFilter{}";
    }

}
