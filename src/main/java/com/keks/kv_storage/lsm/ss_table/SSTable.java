package com.keks.kv_storage.lsm.ss_table;

import com.keks.kv_storage.record.KVRecord;
import com.keks.kv_storage.lsm.io.SSTableReader;
import com.keks.kv_storage.lsm.utils.BloomFilter;
import com.keks.kv_storage.lsm.query.SsTableRangeIterator;
import com.keks.kv_storage.query.range.MaxRangeKey;
import com.keks.kv_storage.query.range.MinRangeKey;
import com.keks.kv_storage.query.range.RangeKey;
import com.keks.kv_storage.query.range.RangeSearchKey;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.concurrent.locks.ReentrantReadWriteLock;


public class SSTable {

    private final ReentrantReadWriteLock rw = new ReentrantReadWriteLock();

    public final int id;

    public final int version;
    public final File ssTableDirPath;
    public final ArrayList<IndexedKey> inMemorySparseIndexes;
    public final SSTableMetadata ssTableMetadata;
    public final BloomFilter bloomFilter;
    private final SSTableReader ssTableReader;
    private final IndexedKey minKey;
    private final IndexedKey maxKey;

    public SSTable(int ssTableId,
                   int version,
                   File ssTableDirPath,
                   SSTableReader ssTableReader,
                   SSTableMetadata ssTableMetadata,
                   BloomFilter bloomFilter) throws IOException {
        this.id = ssTableId;
        this.version = version;
        this.ssTableDirPath = ssTableDirPath;
        this.ssTableReader = ssTableReader;
        this.inMemorySparseIndexes = ssTableReader.readSparseIndex();
        this.ssTableMetadata = ssTableMetadata;
        this.bloomFilter = bloomFilter;
        if (inMemorySparseIndexes.size() == 0) {
            this.minKey = new IndexedKey("", 0, 0);
            this.maxKey = new IndexedKey("", 0, 0);
        } else {
            this.minKey = inMemorySparseIndexes.get(0);
            this.maxKey = inMemorySparseIndexes.get(inMemorySparseIndexes.size() - 1);
        }
    }

    // TODO create class SearchKey and replace String
    public KVRecord searchKey(String key) throws IOException {
        try {
            rw.readLock().lock();
            // key.compareTo(minKey.key) >= 0 -> key is greater then minKey
            if (bloomFilter.contains(key) && key.compareTo(minKey.key) >= 0 && key.compareTo(maxKey.key) <= 0) {
                IndexedKey sparseIndexKey = getSparseIndexForKey(new RangeSearchKey(key, true));
                KVRecord res = null;
                IndexedKey denseIndexedKey = ssTableReader.findKeyInDenseIndex(key, sparseIndexKey.posInReferenceFile);
                if (denseIndexedKey != null) {
                    res = ssTableReader.readKVRecord(denseIndexedKey.posInReferenceFile, denseIndexedKey.refRecordLen);
                }
                return res;
            } else {
                return null;
            }
        } finally {
            rw.readLock().unlock();
        }
    }

    public SsTableRangeIterator searchRange(RangeKey left, RangeKey right) throws IOException {
        rw.readLock().lock();

        if (left.isGreater(maxKey.key) || right.isLower(minKey.key) || inMemorySparseIndexes.size() == 0) {
            rw.readLock().unlock();
            return SsTableRangeIterator.EMPTY; // [a:b] l,r -> l > b || r < a
        } else {

            IndexedKey sparseIndexKey = getSparseIndexForKey(left); // closest key in index
            IndexedKey denseIdxKey = ssTableReader.findKeyInDenseIndex(sparseIndexKey.key, sparseIndexKey.posInReferenceFile);
            long dataFileStartPos = denseIdxKey.posInReferenceFile;
            Iterator<KVRecord> kvRecordIterator = ssTableReader.readRecords(dataFileStartPos);

            return new SsTableRangeIterator(kvRecordIterator, left, right, () -> rw.readLock().unlock());
        }
    }

    public SsTableRangeIterator readAllRecords() throws IOException {
       return searchRange(new MinRangeKey(), new MaxRangeKey());
    }

    private IndexedKey getSparseIndexForKey(RangeKey key) {
        if (key  instanceof MinRangeKey) {
            return inMemorySparseIndexes.get(0);
        } else if (key  instanceof MaxRangeKey) {
            return inMemorySparseIndexes.get(inMemorySparseIndexes.size() - 1);
        } else {
            RangeSearchKey searchKey = (RangeSearchKey) key;
            int arrIndex = Collections.binarySearch(
                    inMemorySparseIndexes,
                    new IndexedKey(searchKey.key, 0, 0),
                    Comparator.comparing(o -> o.key));
            if (arrIndex == -1) {
                arrIndex = 0;
            }
            if (arrIndex < 0) {
                arrIndex = (arrIndex + 2) * -1;
            }
            IndexedKey indexedKey = inMemorySparseIndexes.get(arrIndex);
            return indexedKey;
        }
    }



    public void close() throws IOException {
        try {
            rw.writeLock().lock();
            ssTableReader.close();
        } finally {
            rw.writeLock().unlock();
        }
    }

}

