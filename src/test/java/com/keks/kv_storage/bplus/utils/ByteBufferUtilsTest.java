package com.keks.kv_storage.bplus.utils;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;


class ByteBufferUtilsTest {

    @Test
    public void test1234() {
        byte[] bytes = new byte[199];

        ByteBuffer bb = ByteBuffer.wrap(bytes, 88, 40);
        bb.mark();
        long i = 1;
        while (bb.remaining() > 0) {
            bb.putLong(i++);
        }

        bb.reset();

        while (bb.remaining() > 0) {
            System.out.println(bb.getLong());
        }
        System.out.println();

        ByteBufferUtils.shiftRight(bb, 32, 8);

        while (bb.remaining() > 0) {
            System.out.println(bb.getLong());
        }
        System.out.println();
    }

    @Test
    public void test1235() {
        byte[] bytes = new byte[199];

        ByteBuffer bb = ByteBuffer.wrap(bytes, 88, 40);
        bb.mark();
        long i = 1;
        while (bb.remaining() > 0) {
            bb.putLong(i++);
        }

        bb.reset();

        while (bb.remaining() > 0) {
            System.out.println(bb.getLong());
        }
        System.out.println();

        ByteBufferUtils.shiftLeft(bb, 32, 8);

        while (bb.remaining() > 0) {
            System.out.println(bb.getLong());
        }
    }

    @Test
    public void testShiftRight() {
        byte[] bytes = new byte[199];

        ByteBuffer bb = ByteBuffer.wrap(bytes, 88, 40);
        bb.mark();
        {
            bb.reset();
            long i = 1;
            while (bb.remaining() > 0) bb.putLong(i++);

            ByteBufferUtils.shiftRight(bb, 32, 8);

            bb.reset();
            long[] longs = new long[5];
            for (int j = 0; j < 5; j++) {
                longs[j] = bb.getLong();
            }
            assertArrayEquals(new long[]{1, 2, 3, 4, 5}, longs);
        }
        {
            bb.reset();
            long i = 1;
            while (bb.remaining() > 0) bb.putLong(i++);
            ByteBufferUtils.shiftRight(bb, 0, 8);

            bb.reset();
            long[] longs = new long[5];
            for (int j = 0; j < 5; j++) {
                longs[j] = bb.getLong();
            }
            assertArrayEquals(new long[]{1, 1, 2, 3, 4}, longs);
        }

        {
            bb.reset();
            long i = 1;
            while (bb.remaining() > 0) bb.putLong(i++);
            ByteBufferUtils.shiftRight(bb, 16, 8);

            bb.reset();
            long[] longs = new long[5];
            for (int j = 0; j < 5; j++) {
                longs[j] = bb.getLong();
            }
            assertArrayEquals(new long[]{1, 2, 3, 3, 4}, longs);
        }

    }

    @Test
    public void testShiftLeft() {
        byte[] bytes = new byte[199];

        ByteBuffer bb = ByteBuffer.wrap(bytes, 88, 40);
        bb.mark();
        {
            bb.reset();
            long i = 1;
            while (bb.remaining() > 0) bb.putLong(i++);

            ByteBufferUtils.shiftLeft(bb, 32, 8);

            bb.reset();
            long[] longs = new long[5];
            for (int j = 0; j < 5; j++) {
                longs[j] = bb.getLong();
            }
            assertArrayEquals(new long[]{1, 2, 3, 5, 5}, longs);
        }
        {
            bb.reset();
            long i = 1;
            while (bb.remaining() > 0) bb.putLong(i++);
            ByteBufferUtils.shiftLeft(bb, 40, 8);

            bb.reset();
            long[] longs = new long[5];
            for (int j = 0; j < 5; j++) {
                longs[j] = bb.getLong();
            }
            assertArrayEquals(new long[]{1, 2, 3, 4, 5}, longs);
        }

        {
            bb.reset();
            long i = 1;
            while (bb.remaining() > 0) bb.putLong(i++);
            ByteBufferUtils.shiftLeft(bb, 16, 8);

            bb.reset();
            long[] longs = new long[5];
            for (int j = 0; j < 5; j++) {
                longs[j] = bb.getLong();
            }
            assertArrayEquals(new long[]{1, 3, 4, 5, 5}, longs);
        }

    }

}