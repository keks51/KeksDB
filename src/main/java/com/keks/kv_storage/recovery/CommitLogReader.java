package com.keks.kv_storage.recovery;

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
            CommitLogReadIterator commitLogReadIterator = new CommitLogReadIterator(RecoveryManager.COMMIT_LOG_FILE_NAME + i, fileChannels[i], 10 * 1024 * 1024);
            if (commitLogReadIterator.hasNext()) {
                iterators.add(commitLogReadIterator);
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
                SeqIdKvRecord next = iterator.next();
                queue.add(next);
            } else {
                iteratorsToRemove.add(iterator);
            }
        }
        if (!iteratorsToRemove.isEmpty()) {
            for (CommitLogReadIterator commitLogReadIterator : iteratorsToRemove) {
                iterators.remove(commitLogReadIterator);
            }
            iteratorsToRemove.clear();
        }

    }

    long nextId = 0;

    @Override
    public boolean hasNext() {
        if (queue.isEmpty()) {
            fillQueue();
        }
        boolean queueIsEmpty = queue.isEmpty();
        if (queueIsEmpty) assert iterators.isEmpty();
        return !queueIsEmpty;
    }


    @Override
    public SeqIdKvRecord next() {
        while (queue.peek().id != nextId && !iterators.isEmpty()) { // commit logs file can contain 0,1,2,3,4,6 (5) is missed
            fillQueue();
        }

        SeqIdKvRecord record = queue.poll();
        nextId = record.id;
        return record;
    }

    public void close() throws IOException {
        for (FileChannel fileChannel : fileChannels) {
            fileChannel.close();
        }
    }


}
