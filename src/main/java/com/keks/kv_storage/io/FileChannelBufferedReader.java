package com.keks.kv_storage.io;

import com.keks.kv_storage.Item;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;


public abstract class FileChannelBufferedReader<T extends Item> extends BufferedReader<T> {

    private final FileChannel channel;

    public long filePos;

    public FileChannelBufferedReader(FileChannel channel, long fileStartPos, int bufSize, int thresholdToUpdateBuf) {
        super(bufSize, thresholdToUpdateBuf);
        this.channel = channel;
        this.filePos = fileStartPos;
    }

    @Override
    protected void loadBuffer(ByteBuffer buffer) {
        try {
            filePos = filePos - buffer.remaining();
            buffer.clear();
            int read = channel.read(buffer, filePos);

            filePos = filePos + read;
            buffer.clear();
            buffer.limit(Math.max(0, read)); // if read == -1 then limit(0)
            buffer.mark();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
