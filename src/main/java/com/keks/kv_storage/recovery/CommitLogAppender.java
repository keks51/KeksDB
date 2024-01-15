package com.keks.kv_storage.recovery;

import com.keks.kv_storage.record.KVRecord;
import com.keks.kv_storage.TypeSize;
import com.keks.kv_storage.ex.CommitLogDirAlreadyExists;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.atomic.AtomicLong;


public class CommitLogAppender implements Closeable {


    private final FileChannel[] fileChannels;
    private final AtomicLong atomicLong = new AtomicLong(0L);

    // TODO move StandardOpenOption.SYNC to conf
    public CommitLogAppender(File commitLogDir, int logFiles) throws IOException {
        if (commitLogDir.exists()) throw new CommitLogDirAlreadyExists(commitLogDir);
        commitLogDir.mkdir();
        this.fileChannels = new FileChannel[logFiles];
        for (int i = 0; i < logFiles; i++) {
            fileChannels[i] = FileChannel.open(new File(commitLogDir, RecoveryManager.COMMIT_LOG_FILE_NAME + i).toPath(),
                StandardOpenOption.SYNC, // DSYNC not always persist data
                StandardOpenOption.CREATE_NEW,
                StandardOpenOption.WRITE);
        }
    }
    public void append(KVRecord record) throws IOException {
        long seqId = atomicLong.getAndIncrement();
        SeqIdKvRecord seqIdKvRecord = new SeqIdKvRecord(seqId, record);
//        ByteBuffer bb = ByteBuffer.allocateDirect(TypeSize.INT + TypeSize.LONG + record.getLen());

//        bb.putLong(seqId);
//        record.copyToBB(bb);
//        bb.position(0);

        int fileChannelId = Math.abs(record.hashCode()) % fileChannels.length;
        FileChannel fileChannel = fileChannels[fileChannelId];
        fileChannel.write(ByteBuffer.wrap(seqIdKvRecord.getBytes()));
    }

    public void close() throws IOException {
        for (FileChannel fileChannel : fileChannels) {
            fileChannel.close();
        }
    }

}
