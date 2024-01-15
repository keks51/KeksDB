package com.keks.kv_storage.lsm.io;

import com.keks.kv_storage.record.KVRecord;
import com.keks.kv_storage.TypeSize;
import com.keks.kv_storage.io.FileChannelBufferedReader;
import com.keks.kv_storage.lsm.ss_table.IndexedKey;
import com.keks.kv_storage.record.KvRow;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Iterator;

import static com.keks.kv_storage.lsm.io.SSTableWriter.*;


public class SSTableReader implements AutoCloseable {

    private final FileChannel dataFC;
    private final FileChannel denseIndexFC;
    private final File tableDirPath;
    private final int sparseIndexSize;

    public SSTableReader(File tableDirPath,
                         int sparseIndexSize) throws IOException {
        this.sparseIndexSize = sparseIndexSize;
        this.tableDirPath = tableDirPath;
        this.dataFC = FileChannel.open(new File(tableDirPath, DATA_FILE_NAME).toPath(), StandardOpenOption.READ);
        this.denseIndexFC = FileChannel.open(new File(tableDirPath, DENSE_INDEX_FILE_NAME).toPath(), StandardOpenOption.READ);
    }

    public Iterator<KVRecord> readRecords(long fileStartPos) {
        return new SSTableReader.DataReader(dataFC, fileStartPos, 32 * 1024);
    }

    public IndexedKey findKeyInDenseIndex(String searchedKey, long fileStartPos) throws IOException {
        DenseIndexReader denseIndexReader =
                new DenseIndexReader(denseIndexFC, sparseIndexSize, fileStartPos, 32 * 1024);
        while (denseIndexReader.hasNext()) {
            IndexedKey indexedKey = denseIndexReader.next();
            int compared = searchedKey.compareTo(indexedKey.key);
            if (compared == 0) {
                return indexedKey;
            } else if (compared < 0) {
                return null;
            }
        }
        return null;
    }

    public KVRecord readKVRecord(long pos, int len) throws IOException {
        ByteBuffer bb = ByteBuffer.allocateDirect(len);
        dataFC.read(bb, pos);
        bb.rewind();
        KvRow kvRow = KvRow.fromByteBuffer(bb);
        return new KVRecord(kvRow.keyBytes[0], kvRow.valueBytes[0]);
    }

    public ArrayList<IndexedKey> readSparseIndex() throws IOException {
        ArrayList<IndexedKey> sparseIndexArr = new ArrayList<>();
        try (FileChannel partialIndexChannel = FileChannel.open(new File(tableDirPath, SPARSE_INDEX_FILE_NAME).toPath(), StandardOpenOption.READ)) {
            SparseIndexReader sparseIndexReader = new SparseIndexReader(partialIndexChannel, 0, 32 * 1024);
            while (sparseIndexReader.hasNext()) {
                IndexedKey next = sparseIndexReader.next();
                sparseIndexArr.add(next);
            }
        }
        return sparseIndexArr;
    }

    public static class SparseIndexReader extends FileChannelBufferedReader<IndexedKey> {

        public SparseIndexReader(FileChannel channel, long filePos, int bufSize) {
            super(channel, filePos, bufSize, TypeSize.SHORT);
        }

        @Override
        protected IndexedKey buildRecord(ByteBuffer bb) {
            return IndexedKey.fromByteBuffer(bb);
        }

        @Override
        protected int calcRecordLen(ByteBuffer bb) {
            try {
                bb.mark();
                return bb.getShort();
            } finally {
                bb.reset();
            }
        }

    }

    // TODO key max size should be 1024 chars
    public static class DenseIndexReader extends FileChannelBufferedReader<IndexedKey> {
        private final int recInBlock;

        public DenseIndexReader(FileChannel channel, int recInBlock, long fileStartPos, int bufSize) {
            super(channel, fileStartPos, bufSize, TypeSize.SHORT);
            this.recInBlock = recInBlock;
        }

        @Override
        public boolean hasNext() {
            return (super.readRec < recInBlock && super.hasNext());
        }

        @Override
        protected IndexedKey buildRecord(ByteBuffer bb) {
            return IndexedKey.fromByteBuffer(bb);
        }

        @Override
        protected int calcRecordLen(ByteBuffer bb) {
            try {
                bb.mark();
                return bb.getShort();
            } finally {
                bb.reset();
            }
        }

    }

    public static class DataReader extends FileChannelBufferedReader<KVRecord> {

        public DataReader(FileChannel channel, long fileStartPos, int bufSize) {
            super(channel, fileStartPos, bufSize, TypeSize.INT);
        }

        @Override
        protected KVRecord buildRecord(ByteBuffer bb) {
//            return KVRecord.fromByteBuffer(bb);
            KvRow kvRow = KvRow.fromByteBuffer(bb);
            return new KVRecord(kvRow.keyBytes[0], kvRow.valueBytes[0]);
        }

        @Override
        protected int calcRecordLen(ByteBuffer bb) {
            try {
                bb.mark();
                return bb.getInt();
            } finally {
                bb.reset();
            }
        }
    }

    public void close() throws IOException {
        dataFC.close();
        denseIndexFC.close();
    }

}
