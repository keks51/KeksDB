package com.keks.kv_storage.recovery;

import com.keks.kv_storage.record.KVRecord;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.PriorityQueue;


public class CommitLogReader implements Iterator<SeqIdKvRecord>, Closeable {

    private final FileChannel[] fileChannels;

    final HashSet<CommitLogReadIterator> iterators;
    private final LinkedList<CommitLogReadIterator> iteratorsToRemove = new LinkedList<>();
    private final PriorityQueue<SeqIdKvRecord> queue;

    public CommitLogReader(File commitLogDir, int logFiles) throws IOException {
        this.fileChannels = new FileChannel[logFiles];
        for (int i = 0; i < logFiles; i++) {
            fileChannels[i] = FileChannel.open(new File(commitLogDir, RecoveryManager.COMMIT_LOG_FILE_NAME + i).toPath(), StandardOpenOption.READ);
        }
        this.queue = new PriorityQueue<>(logFiles * 10);
        this.iterators = new HashSet<>(logFiles);
        for (int i = 0; i < logFiles; i++) {
            CommitLogReadIterator CommitLogReadIterator = new CommitLogReadIterator(RecoveryManager.COMMIT_LOG_FILE_NAME + i, fileChannels[i], 10 * 1024 * 1024);
            if (CommitLogReadIterator.hasNext()) {
                iterators.add(CommitLogReadIterator);
            }
        }

        int initQueueSize = logFiles * 10;
        while (!iterators.isEmpty() && queue.size() < initQueueSize) {
            fillQueue();
        }
    }


    private void fillQueue() {
        for (CommitLogReadIterator iterator : iterators) {
            if (iterator.hasNext()) {
                KvRecordsBatch next = iterator.next();
                long id = 0;
                for (KVRecord kvRecord : next.kvRecords) {
                    queue.add(new SeqIdKvRecord(next.batchId, id, kvRecord));
                    id++;
                }

            } else {
                iteratorsToRemove.add(iterator);
            }
        }
        if (!iteratorsToRemove.isEmpty()) {
            for (CommitLogReadIterator CommitLogReadIterator : iteratorsToRemove) {
                iterators.remove(CommitLogReadIterator);
            }
            iteratorsToRemove.clear();
        }

    }

    long nextSeqId = 0;
    int nextBatchId = 0;

    @Override
    public boolean hasNext() {
        if (queue.isEmpty()) {
            fillQueue();
        }
        boolean queueIsEmpty = queue.isEmpty();
        if (queueIsEmpty) assert iterators.isEmpty();
        return !queueIsEmpty;
    }

    //batch_id,seq_id
    //   0       0
    //   0       1
    //   0       2
    //


    @Override
    public SeqIdKvRecord next() {
        if (queue.peek().id == nextSeqId && queue.peek().batchId == nextBatchId) {

        } else {
            nextBatchId = nextBatchId + 1;
            nextSeqId = 0;
            while (queue.peek().batchId != nextBatchId && !iterators.isEmpty()) { // commit logs file can contain 0,1,2,3,4,6 (5) is missed
                fillQueue();
            }
        }


//        while (queue.peek().batchId != nextBatchId && !iterators.isEmpty()) {
//            fillQueue();
//
//        }

//        while (queue.peek().batchId != nextBatchId && queue.peek().id != nextSeqId && !iterators.isEmpty()) { // commit logs file can contain 0,1,2,3,4,6 (5) is missed
//            fillQueue();
//        }

        SeqIdKvRecord record = queue.poll();
//        System.out.println(record.batchId + "_" + nextBatchId + "_" + iterators.isEmpty() + "_" + queue.peek());
        if (queue.peek() != null && record.batchId + 1 != queue.peek().batchId){
//            System.out.println();
        }
        nextSeqId = record.id + 1;
        return record;
    }

    public void close() throws IOException {
        for (FileChannel fileChannel : fileChannels) {
            fileChannel.close();
        }
    }


}
