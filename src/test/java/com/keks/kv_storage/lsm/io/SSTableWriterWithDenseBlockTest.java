package com.keks.kv_storage.lsm.io;

import com.keks.kv_storage.lsm.conf.LsmConf;
import com.keks.kv_storage.lsm.ss_table.DenseIndexBlock;
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
import java.util.Collections;
import java.util.Comparator;

import static com.keks.kv_storage.lsm.io.SSTableWriter.*;
import static org.junit.jupiter.api.Assertions.*;


class SSTableWriterWithDenseBlockTest {

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 3, 4, 5, 6, 25, 100, 498, 499, 500, 501, 502, 503})
    public void test1(int numberOfKeysInIndex, @TempDir Path parentDir) throws IOException {

        LsmConf lsmConf = new LsmConf(numberOfKeysInIndex, 10, 0.5);

        for (int recordsCnt = 1; recordsCnt < 500; recordsCnt++) {
            Path dir = parentDir.resolve(recordsCnt + "");
            dir.toFile().mkdir();

//        int recordsCnt = 4;
            SSTableWriterWithDenseBlock ssTableWriter = new SSTableWriterWithDenseBlock(dir.toFile(), lsmConf, recordsCnt);

            int expFileSize = 0;
            ArrayList<KVRecord> list = new ArrayList<>(recordsCnt);
            for (int i = 0; i < recordsCnt; i++) {
                KVRecord rec = new KVRecord("key" + i, ("value" + i).getBytes());
                expFileSize += rec.getLen();
                list.add(rec);
            }
            Collections.sort(list, Comparator.comparing(o -> o.key));
            ssTableWriter.createSSTable(list.iterator());


            // assert data

            ByteBuffer dataBB = readFileInBuffer(new File(dir.toFile(), DATA_FILE_NAME));
            ByteBuffer indexBB = readFileInBuffer(new File(dir.toFile(), DENSE_INDEX_FILE_NAME)); // since index file is lower then data
            ByteBuffer sparseIndexBB = readFileInBuffer(new File(dir.toFile(), SPARSE_INDEX_FILE_NAME)); // since partial index file is lower then data


            // check idx block and sparse idx
            int totalKeysInDenseIdx = 0;
            DenseIndexBlock denseIndexBlock = null;
            indexBB.rewind();
            ArrayList<IndexedKey> spareKeys = new ArrayList<>();
            for (int keyId = 0; keyId < recordsCnt; keyId++) {
                // load new idx block
                if (keyId % numberOfKeysInIndex == 0) {
                    denseIndexBlock = DenseIndexBlock.fromByteBuffer(indexBB);
                    totalKeysInDenseIdx += denseIndexBlock.getKeysNum();
                    IndexedKey sparseKey = IndexedKey.fromByteBuffer(sparseIndexBB);
                    spareKeys.add(sparseKey);
                    assertEquals(sparseKey.key, denseIndexBlock.getKey(0).key);

                }
                KVRecord expLsmRec = list.get(keyId);
                KvRow kvRow = KvRow.fromByteBuffer(dataBB);

                KVRecord actLsmRec = new KVRecord(kvRow.keyBytes[0], kvRow.valueBytes[0]);
                assertEquals(expLsmRec, actLsmRec);

                try {
                    assertEquals(expLsmRec.key, denseIndexBlock.findKey(expLsmRec.key).key);
                } catch (Throwable e) {
                    System.out.println();
                }
            }

            // check extra sparse key when last DOESNT't contains 1 key
            if (recordsCnt == 1 || denseIndexBlock.getKeysNum() > 1) {
                try {
                    IndexedKey sparseKey = IndexedKey.fromByteBuffer(sparseIndexBB);
                    assertEquals(sparseKey.key, denseIndexBlock.getKey(denseIndexBlock.getKeysNum() - 1).key);
                }catch (Throwable e) {
                    System.out.println();
                }

            }

            assertEquals(recordsCnt, totalKeysInDenseIdx);
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