package com.keks.kv_storage.recovery;

import com.keks.kv_storage.TypeSize;
import com.keks.kv_storage.io.FileChannelBufferedReader;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;


public class CommitLogReadIterator extends FileChannelBufferedReader<KvRecordsBatch> {

    public CommitLogReadIterator(FileChannel channel, int bufSize) {
        super(channel, 0, bufSize, TypeSize.LONG + TypeSize.INT);
    }

    public CommitLogReadIterator(String fileName, FileChannel channel, int bufSize) {
        super(channel, 0, bufSize, TypeSize.LONG + TypeSize.INT);
    }

    @Override
    protected KvRecordsBatch buildRecord(ByteBuffer bb) {
        return KvRecordsBatch.fromByteBuffer(bb);
    }

    @Override
    protected int calcRecordLen(ByteBuffer bb) {
        try {
            bb.mark();
            int anInt = bb.getInt();
            return anInt;
        }finally {
            bb.reset();
        }
    }

}
