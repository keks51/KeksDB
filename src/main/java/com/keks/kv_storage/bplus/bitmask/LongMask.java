package com.keks.kv_storage.bplus.bitmask;


public class LongMask {

    public static int BITS_IN_MASK = 64;
    private int setBitsCnt = 0;

    public static long MASK_IS_FULL_LONG_VALUE = -1;

    public static long MASK_IS_EMPTY_LONG_VALUE = 0L;

    protected Long mask;

    public LongMask(long mask) {
        this.mask = mask;
    }

    public boolean isFull() {
        return mask == MASK_IS_FULL_LONG_VALUE;
    }

    public int getFirstUnsetFromRight() {
        if (mask == MASK_IS_FULL_LONG_VALUE) { // all bits are set 11111.....111
            return -1;
        } else {
            int res;
            if (mask == 9223372036854775807L) {
                res = 63; // during calculations n becomes negative and log cannot be calculated
            } else {
                res = (int) ((Math.log10((mask & ~(mask + 1)) + 1) / Math.log10(2))); // log10(16)/log10(2) => log2(16)
            }
            set(res);
            setBitsCnt++;
            return res;
        }
    }

    public boolean isSet(long pos) {
        long o = 1L << pos;
        long t = mask & o;
        return t != 0;
    }

    public void set(long pos) {
        long o = 1L << pos;
        mask = mask | o;
        setBitsCnt++;
    }

    public boolean unSet(long pos) {
        if (isSet(pos)) {
            long o = 1L << pos;
            mask = mask ^ o;
            setBitsCnt--;
            return true;
        }
        return false;
    }

    public void clear() {
        setBitsCnt = 0;
        mask = MASK_IS_EMPTY_LONG_VALUE;
    }

    @Override
    public String toString() {
        return String.format("%64s", Long.toBinaryString(mask)).replace(' ', '0');
    }
}
