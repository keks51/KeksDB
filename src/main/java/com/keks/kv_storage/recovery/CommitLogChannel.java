package com.keks.kv_storage.recovery;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;


public class CommitLogChannel implements AutoCloseable {

    private final FileChannel fileChannel;
    private final AtomicLong curPosLong;

    private final ReentrantLock lock = new ReentrantLock();

    public CommitLogChannel(FileChannel fileChannel) {
        this.fileChannel = fileChannel;
        this.curPosLong = new AtomicLong();
    }

    public void write(byte[] bytes) throws IOException {
        ByteBuffer bb = ByteBuffer.wrap(bytes);
        int size = bytes.length;
        long prevPos = curPosLong.getAndAdd(size);
        lock.lock(); //  TODO with outer lock is twice faster than rely on internal lock. IDK. Test with huge bytes arr
        fileChannel.write(bb, prevPos);
        lock.unlock();
    }

    @Override
    public void close() throws IOException {
        fileChannel.close();
    }
}