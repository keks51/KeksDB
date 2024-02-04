package com.keks.kv_storage.lsm.io;

import com.keks.kv_storage.lsm.conf.LsmConf;
import com.keks.kv_storage.lsm.ss_table.DenseIndexBlock;
import com.keks.kv_storage.lsm.ss_table.IndexedKey;
import com.keks.kv_storage.lsm.ss_table.SSTableMetadata;
import com.keks.kv_storage.lsm.utils.BloomFilter;
import com.keks.kv_storage.record.KVRecord;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Iterator;


public class SSTableWriterWithDenseBlock {

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

    public SSTableWriterWithDenseBlock(File tableDirPath,
                                       LsmConf lsmConf,
                                       int approximateKeyCount) {
        this.tableDirPath = tableDirPath;
        this.sparseIndexSize = lsmConf.sparseIndexSize;
        this.bloomFilter = new BloomFilter(approximateKeyCount, lsmConf.bloomFilterFalsePositiveRate);
    }

    public void createSSTable(Iterator<KVRecord> records) throws IOException {
        long dataRecStartOffset = 0;
        long denseBlockIndexStartOffset = 0;
        int keysInCurrentBlock = 0;
        File dataFileTmp = new File(tableDirPath, DATA_TMP_FILE_NAME);
        File denseIndexFileTmp = new File(tableDirPath, DENSE_INDEX_TMP_FILE_NAME);
        File sparseIndexFileTmp = new File(tableDirPath, SPARSE_INDEX_TMP_FILE_NAME);
        DenseIndexBlock.DenseIndexBlockBuilder denseIdxblockBuilder = new DenseIndexBlock.DenseIndexBlockBuilder(sparseIndexSize);

        try (BufferedOutputStream dataOS = new BufferedOutputStream(new FileOutputStream(dataFileTmp));
             BufferedOutputStream denseIndexOS = new BufferedOutputStream(new FileOutputStream(denseIndexFileTmp));
             BufferedOutputStream sparseIndexOS = new BufferedOutputStream(new FileOutputStream(sparseIndexFileTmp));
             BloomFilterRafRW bloomFilterWriter = new BloomFilterRafRW(tableDirPath);
             MetadataJsonRW metadataWriter = new MetadataJsonRW(tableDirPath)
        ) {
            IndexedKey indexedKey = null;
            String sparseKeyStr = null;
            while (records.hasNext()) {
                // while block idx is not full
                while (records.hasNext() && keysInCurrentBlock < sparseIndexSize) {
                    KVRecord record = records.next();
                    // adding to DataFile
                    dataOS.write(record.getBytes());

                    // adding to DenseIndexFile
                    indexedKey = new IndexedKey(record.key, dataRecStartOffset, record.getLen());
                    dataRecStartOffset += record.getLen();
                    denseIdxblockBuilder.add(indexedKey);
                    if (keysInCurrentBlock == 0) { // first key in block is sparse idx
                        sparseKeyStr = indexedKey.key;
                    }

                    // updating bloomFilter
                    bloomFilter.add(record.key);

                    // collecting metadata
                    totalNumberOfRecords += 1;
                    if (record.isDeleted()) deletedRecords += 1;
                    keysInCurrentBlock++;
                }


                // Adding to SparseIndexFile if block is full or no more records
                if (keysInCurrentBlock == sparseIndexSize || !records.hasNext()) {
                    // writing dense idx block
                    DenseIndexBlock denseIndexBlock = denseIdxblockBuilder.build();
                    denseIndexOS.write(denseIndexBlock.getBytes());

                    // writing sparse idx
                    assert sparseKeyStr != null;
                    IndexedKey sparseIndexedKey = new IndexedKey(sparseKeyStr, denseBlockIndexStartOffset, denseIndexBlock.getLen());
                    sparseIndexOS.write(sparseIndexedKey.getBytes());

                    // add last indexed key as MAX key to sparse idx only IF
                    // all records are written AND (only one record || last dense block contains more than one record)
                    if (!records.hasNext() && (totalNumberOfRecords == 1 || denseIndexBlock.getKeysNum() > 1)) {
                        // writing last indexed key. This key will be read as max key. First index key is a min key
                        if (totalNumberOfRecords == 1 || denseIndexBlock.getKeysNum() > 1) {
                            IndexedKey sparseMaxIndexedKey = new IndexedKey(indexedKey.key, denseBlockIndexStartOffset, denseIndexBlock.getLen());
                            sparseIndexOS.write(sparseMaxIndexedKey.getBytes());
                        }
                    }

                    // creating new denseIdx builder and setting new block start offset
                    denseBlockIndexStartOffset += denseIndexBlock.getLen();
                    denseIdxblockBuilder = new DenseIndexBlock.DenseIndexBlockBuilder(sparseIndexSize);
                    keysInCurrentBlock = 0;
                }
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
