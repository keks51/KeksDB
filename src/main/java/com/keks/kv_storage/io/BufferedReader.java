package com.keks.kv_storage.io;

import com.keks.kv_storage.Item;

import java.nio.ByteBuffer;
import java.util.Iterator;


public abstract class BufferedReader<T extends Item> implements Iterator<T> {
    public static int MIN_BUF_SIZE = 4 * 1024; // 4kb

    private final int thresholdToUpdateBuf;

    protected int readRec = 0;
    private final ByteBuffer buffer;
    private final int bufSize;

//    private boolean stop = false;

    public BufferedReader(int bufSize, int thresholdToUpdateBuf) {
        assert thresholdToUpdateBuf < bufSize;
        this.buffer = ByteBuffer.allocate(bufSize);
        this.bufSize = bufSize;
        buffer.clear();
        buffer.limit(0);
        this.thresholdToUpdateBuf = thresholdToUpdateBuf;

    }



    @Override
    public boolean hasNext() {
//        if (stop) return false; // is needed since for example forEach triggers hasNext 2 times and if readToBuffer channel or stream is closed then exception is thrown

        if (buffer.remaining() <= thresholdToUpdateBuf) { // TODO if record len is 4 bytes aka int then thresholdToUpdateBuf should be 4 bytes. think how to make general solution
            readToBuffer(buffer);
        }

        boolean hasNext = buffer.remaining() > 0;
//        stop = !hasNext;
        return hasNext;
    }

    @Override
    public T next() {
        readRec++;

        int remaining = buffer.remaining(); // TODO delete
        int recordLen = calcRecordLen(buffer);
        if (recordLen > remaining) {
            readToBuffer(buffer);
        }
        if (recordLen > bufSize) {
            ByteBuffer byteBuffer = allocateHugeBuffer(recordLen);
            byteBuffer.position(0);
            return buildRecord(byteBuffer);
        } else {
            return buildRecord(buffer);
        }
    }

    public int getReadRecordsCnt() {
        return readRec;
    }

    protected abstract T buildRecord(ByteBuffer bb);

    protected abstract int calcRecordLen(ByteBuffer bb);


    protected abstract void readToBuffer(ByteBuffer buffer);

    private ByteBuffer allocateHugeBuffer(int size) {
        ByteBuffer hugeBuffer = ByteBuffer.allocateDirect(size);
        while (hugeBuffer.position() != size) {
            while (buffer.remaining() > 0 && hugeBuffer.remaining() > 0) {
                hugeBuffer.put(buffer.get());
            }

            if (hugeBuffer.remaining() > 0) {
                readToBuffer(buffer);
            }
        }

        hugeBuffer.clear();
        return hugeBuffer;
    }

}
