package com.keks.kv_storage.bplus.utils;

import java.nio.ByteBuffer;

public class ByteBufferUtils {

    public static void shiftRight(ByteBuffer bb, int srcPos, int shiftLen) {
        bb.reset();
        byte[] array = bb.array();
        int arrSrc = srcPos + bb.position();
        int arrDst = arrSrc + shiftLen;
        int limit = bb.limit();
        int len = limit - arrDst;
        if (len != 0) System.arraycopy(array, arrSrc, array, arrDst, len);
    }

    public static void shiftLeft(ByteBuffer bb, int srcPos, int shiftLen) {
        bb.reset();
        byte[] array = bb.array();
        int arrSrc = srcPos + bb.position();
        int arrDst = arrSrc - shiftLen;
        int limit = bb.limit();
        int len = limit - arrSrc;
        if (len != 0) System.arraycopy(array, arrSrc, array, arrDst, len);
    }

}
