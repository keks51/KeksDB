package com.keks.kv_storage.lsm.io;

import com.keks.kv_storage.lsm.conf.LsmConf;
import com.keks.kv_storage.lsm.ss_table.IndexedKey;
import com.keks.kv_storage.record.KVRecord;
import com.keks.kv_storage.record.KvRow;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;

import static com.keks.kv_storage.lsm.io.SSTableWriter.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;


class SSTableWriterTest {

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 3, 4, 5, 6, 25, 100, 498, 499, 500, 501, 502, 503})
    public void test1(int numberOfKeysInIndex, @TempDir Path dirParent) throws IOException {


        for (int recordsCnt = 1; recordsCnt < 500; recordsCnt++) {
            Path dir = dirParent.resolve(numberOfKeysInIndex + "").resolve(recordsCnt + "");
            dir.toFile().mkdirs();
            LsmConf lsmConf = new LsmConf(numberOfKeysInIndex, 10, 0.5);


            SSTableWriter ssTableWriter = new SSTableWriter(dir.toFile(), lsmConf, recordsCnt);

            ArrayList<KVRecord> list = new ArrayList<>(recordsCnt);
            for (int i = 0; i < recordsCnt; i++) {
                KVRecord rec = new KVRecord("key" + i, ("value" + i).getBytes());
                list.add(rec);
            }
            ssTableWriter.createSSTable(list.iterator());


            // assert data
            ByteBuffer dataBB = readFileInBuffer(new File(dir.toFile(), DATA_FILE_NAME));
            ByteBuffer indexBB = readFileInBuffer(new File(dir.toFile(), DENSE_INDEX_FILE_NAME));
            ByteBuffer sparseIndexBB = readFileInBuffer(new File(dir.toFile(), SPARSE_INDEX_FILE_NAME));
            long posInDataFile = 0;
            long posInIndexFile = 0;
            int recordsInLastBlockCnt = 0;
            ArrayList<IndexedKey> sparseIdxKeys = new ArrayList<>();
            for (int keyId = 0; keyId < recordsCnt; keyId++) {
                KVRecord expLsmRec = list.get(keyId);
                KvRow kvRow = KvRow.fromByteBuffer(dataBB);

                KVRecord actLsmRec = new KVRecord(kvRow.keyBytes[0], kvRow.valueBytes[0]);
                assertEquals(expLsmRec, actLsmRec);

                IndexedKey expIndexedKey = new IndexedKey(expLsmRec.key, posInDataFile, expLsmRec.getLen());
                IndexedKey actIndexedKey = IndexedKey.fromByteBuffer(indexBB);
                assertEquals(expIndexedKey, actIndexedKey);

                if (keyId % numberOfKeysInIndex == 0) {
                    IndexedKey expSparseIndexedKey = new IndexedKey(expLsmRec.key, posInIndexFile, expIndexedKey.getLen());
                    IndexedKey actSparseIndexedKey = IndexedKey.fromByteBuffer(sparseIndexBB);
                    sparseIdxKeys.add(actSparseIndexedKey);
                    assertEquals(expSparseIndexedKey, actSparseIndexedKey);
                    recordsInLastBlockCnt = 0;
                }
                recordsInLastBlockCnt += 1;
                posInDataFile += actLsmRec.getLen();
                posInIndexFile += actIndexedKey.getLen();
            }

            // check extra sparse key when last DOESNT't contains 1 key
            int numberOfKeysInLastBlock = numberOfKeysInIndex - recordsCnt / numberOfKeysInIndex;
            if (recordsCnt == 1 || recordsInLastBlockCnt > 1) {
                try {
                    IndexedKey sparseKey = IndexedKey.fromByteBuffer(sparseIndexBB);
                    sparseIdxKeys.add(sparseKey);
                    assertEquals(sparseKey.key, list.get(list.size() - 1).key);
                } catch (Throwable e) {
                    System.out.println();
                    throw e;
                }
            }


//        assertEquals(recordsCnt, totalKeysInDenseIdx);
            assertFalse(dataBB.hasRemaining());
            assertFalse(indexBB.hasRemaining());
            assertFalse(sparseIndexBB.hasRemaining());
        }

    }

    private static ByteBuffer readFileInBuffer(File file) throws IOException {
        ByteBuffer bb = ByteBuffer.allocateDirect((int) file.length());
        try (FileChannel channel = FileChannel.open(file.toPath(), StandardOpenOption.READ)) {
            channel.read(bb, 0);
        }
        bb.rewind();
        return bb;
    }


}