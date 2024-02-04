package com.keks.kv_storage.recovery;

import com.keks.kv_storage.ex.CommitLogDirAlreadyExists;
import com.keks.kv_storage.record.KVRecord;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;


public class CommitLogAppender implements Closeable {


    private final CommitLogChannel[] fileChannels;
    private final AtomicInteger batchId = new AtomicInteger(0);

    // TODO move StandardOpenOption.SYNC to conf
    public CommitLogAppender(File commitLogDir, int logFiles) throws IOException {
        if (commitLogDir.exists()) throw new CommitLogDirAlreadyExists(commitLogDir);
        commitLogDir.mkdir();
        this.fileChannels = new CommitLogChannel[logFiles];
        for (int i = 0; i < logFiles; i++) {
            fileChannels[i] = new CommitLogChannel(
                    FileChannel.open(new File(commitLogDir, RecoveryManager.COMMIT_LOG_FILE_NAME + i).toPath(),
                            StandardOpenOption.SYNC, // DSYNC not always persist data
                            StandardOpenOption.CREATE_NEW,
                            StandardOpenOption.WRITE));
        }
    }

    public void append(KVRecord kvRecord) throws IOException {
        ArrayList<KVRecord> list = new ArrayList<>(){{add(kvRecord);}};
        appendBatch(list, kvRecord.getLen());
    }

    public void appendBatch(ArrayList<KVRecord> records, int size) throws IOException {
        KvRecordsBatch kvRecordsBatch = new KvRecordsBatch(batchId.getAndIncrement(), records, size);

        int fileChannelId = Math.abs(records.hashCode()) % fileChannels.length;
        CommitLogChannel fileChannel = fileChannels[fileChannelId];
        fileChannel.write(kvRecordsBatch.getBytes());
    }

    public void close() throws IOException {
        for (CommitLogChannel fileChannel : fileChannels) {
            fileChannel.close();
        }
    }


}
