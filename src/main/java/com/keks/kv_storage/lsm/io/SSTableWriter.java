package com.keks.kv_storage.lsm.io;

import com.keks.kv_storage.record.KVRecord;
import com.keks.kv_storage.lsm.conf.LsmConf;
import com.keks.kv_storage.lsm.utils.BloomFilter;
import com.keks.kv_storage.lsm.ss_table.IndexedKey;
import com.keks.kv_storage.lsm.ss_table.SSTableMetadata;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Iterator;


public class SSTableWriter {

    public static final String TMP_POSTFIX = ".tmp";
    public static final String DATA_FILE_NAME = "data.db";
    public static final String DENSE_INDEX_FILE_NAME = "dense-index.db";
    public static final String SPARSE_INDEX_FILE_NAME = "sparse-index.db";
    private static final String DATA_TMP_FILE_NAME = DATA_FILE_NAME + TMP_POSTFIX;
    private static final String DENSE_INDEX_TMP_FILE_NAME = DENSE_INDEX_FILE_NAME + TMP_POSTFIX;
    private static final String SPARSE_INDEX_TMP_FILE_NAME = SPARSE_INDEX_FILE_NAME + TMP_POSTFIX;

    private final int sparseIndexSize;
    private int totalNumberOfRecords;
    private int deletedRecords;
    private final File tableDirPath;

    private final BloomFilter bloomFilter;

    public SSTableWriter(File tableDirPath,
                         LsmConf lsmConf,
                         int approximateKeyCount) {
        this.tableDirPath = tableDirPath;
        this.sparseIndexSize = lsmConf.sparseIndexSize;
        this.bloomFilter = new BloomFilter(approximateKeyCount, lsmConf.bloomFilterFalsePositiveRate);
    }

    public void createSSTable(Iterator<KVRecord> records) throws IOException {
        long dataRecOffset = 0;
        long denseIndexRecOffset = 0;
        int sparseIndexedKeyToWriteCntDown = 0;
        File dataFileTmp = new File(tableDirPath, DATA_TMP_FILE_NAME);
        File denseIndexFileTmp = new File(tableDirPath, DENSE_INDEX_TMP_FILE_NAME);
        File sparseIndexFileTmp = new File(tableDirPath, SPARSE_INDEX_TMP_FILE_NAME);

        try (BufferedOutputStream dataOS = new BufferedOutputStream(new FileOutputStream(dataFileTmp));
             BufferedOutputStream denseIndexOS = new BufferedOutputStream(new FileOutputStream(denseIndexFileTmp));
             BufferedOutputStream sparseIndexOS = new BufferedOutputStream(new FileOutputStream(sparseIndexFileTmp));
             BloomFilterRafRW bloomFilterWriter = new BloomFilterRafRW(tableDirPath);
             MetadataJsonRW metadataWriter = new MetadataJsonRW(tableDirPath)
        ) {
            IndexedKey indexedKey = null;
            while (records.hasNext()) {
                KVRecord record = records.next();

                // adding to DataFile
                dataOS.write(record.getBytes());

                // adding to DenseIndexFile
                indexedKey = new IndexedKey(record.key, dataRecOffset, record.getLen());
                denseIndexOS.write(indexedKey.getBytes());
                dataRecOffset += record.getLen();

                // Adding to SparseIndexFile
                if (sparseIndexedKeyToWriteCntDown == 0) {
                    IndexedKey sparseIndexedKey = new IndexedKey(indexedKey.key, denseIndexRecOffset, indexedKey.getLen());
                    sparseIndexOS.write(sparseIndexedKey.getBytes());
                    sparseIndexedKeyToWriteCntDown = sparseIndexSize;
                }
                sparseIndexedKeyToWriteCntDown--;
                denseIndexRecOffset += indexedKey.getLen();

                // updating bloomFilter
                bloomFilter.add(record.key);

                // collecting metadata
                totalNumberOfRecords += 1;
                if (record.isDeleted()) deletedRecords += 1;
            }
            // writing last indexed key. This key will be read as max key. First index key is a min key
            if (indexedKey != null && sparseIndexedKeyToWriteCntDown != sparseIndexSize - 1) {
                IndexedKey sparseIndexedKey = new IndexedKey(indexedKey.key, denseIndexRecOffset - indexedKey.getLen(), indexedKey.getLen());
                sparseIndexOS.write(sparseIndexedKey.getBytes());
            }

            SSTableMetadata ssTableMetadata = new SSTableMetadata(sparseIndexSize, totalNumberOfRecords, deletedRecords);
            metadataWriter.write(ssTableMetadata);
            bloomFilterWriter.write(bloomFilter);
        }

        Files.move(dataFileTmp.toPath(), new File(tableDirPath, DATA_FILE_NAME).toPath());
        Files.move(denseIndexFileTmp.toPath(), new File(tableDirPath, DENSE_INDEX_FILE_NAME).toPath());
        Files.move(sparseIndexFileTmp.toPath(), new File(tableDirPath, SPARSE_INDEX_FILE_NAME).toPath());
    }

}
