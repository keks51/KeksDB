package com.keks.kv_storage.recovery;

import com.keks.kv_storage.TypeSize;
import com.keks.kv_storage.io.FileChannelBufferedReader;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;


public class CommitLogReadIterator extends FileChannelBufferedReader<SeqIdKvRecord> {

    public CommitLogReadIterator(FileChannel channel, int bufSize) {
        super(channel, 0, bufSize, TypeSize.LONG + TypeSize.INT);
    }

    public CommitLogReadIterator(String fileName, FileChannel channel, int bufSize) {
        super(channel, 0, bufSize, TypeSize.LONG + TypeSize.INT);
    }

    @Override
    protected SeqIdKvRecord buildRecord(ByteBuffer bb) {
        return SeqIdKvRecord.fromByteBuffer(bb);
    }

    @Override
    protected int calcRecordLen(ByteBuffer bb) {
        try {
            bb.mark();
            return bb.getInt();
        }finally {
            bb.reset();
        }
    }

}
