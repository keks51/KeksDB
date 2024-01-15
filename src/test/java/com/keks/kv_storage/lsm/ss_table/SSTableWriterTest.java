package com.keks.kv_storage.lsm.ss_table;

import com.keks.kv_storage.record.KVRecord;
import com.keks.kv_storage.lsm.conf.LsmConf;
import com.keks.kv_storage.lsm.io.SSTableWriter;
import com.keks.kv_storage.record.KvRow;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;

import static com.keks.kv_storage.lsm.io.SSTableWriter.*;
import static org.junit.jupiter.api.Assertions.assertEquals;


class SSTableWriterTest {

    @Test
    public void test1(@TempDir Path dir) throws IOException {
        int numberOfKeysInIndex = 3;
        LsmConf lsmConf = new LsmConf(numberOfKeysInIndex, 10, 0.5);

        int recordsCnt = 10_000;
        SSTableWriter ssTableWriter = new SSTableWriter(dir.toFile(), lsmConf, recordsCnt);

        int expFileSize = 0;
        ArrayList<KVRecord> list = new ArrayList<>(recordsCnt);
        for (int i = 0; i < recordsCnt; i++) {
            KVRecord rec = new KVRecord("key" + i, ("value" + i).getBytes());
            expFileSize += rec.getLen();
            list.add(rec);
        }
        ssTableWriter.createSSTable(list.iterator());


        // assert data

        ByteBuffer dataBB = readFileInBuffer(new File(dir.toFile(), DATA_FILE_NAME), expFileSize);
        ByteBuffer indexBB = readFileInBuffer(new File(dir.toFile(), DENSE_INDEX_FILE_NAME), expFileSize); // since index file is lower then data
        ByteBuffer partialIndexBB = readFileInBuffer(new File(dir.toFile(), SPARSE_INDEX_FILE_NAME), expFileSize); // since partial index file is lower then data
        long posInDataFile = 0;
        long posInIndexFile = 0;
        for (int i = 0; i < recordsCnt; i++) {
            KVRecord expLsmRec = list.get(i);
            KvRow kvRow = KvRow.fromByteBuffer(dataBB);

            KVRecord actLsmRec = new KVRecord(kvRow.keyBytes[0], kvRow.valueBytes[0]);
            assertEquals(expLsmRec, actLsmRec);

            IndexedKey expIndexedKey = new IndexedKey(expLsmRec.key, posInDataFile, expLsmRec.getLen());
            IndexedKey actIndexedKey = IndexedKey.fromByteBuffer(indexBB);
            assertEquals(expIndexedKey, actIndexedKey);

            if (recordsCnt % numberOfKeysInIndex == 0) {
                IndexedKey expPartialIndexedKey = new IndexedKey(expLsmRec.key, posInIndexFile, expIndexedKey.getLen());
                IndexedKey actPartialIndexedKey = IndexedKey.fromByteBuffer(partialIndexBB);
                assertEquals(expPartialIndexedKey, actPartialIndexedKey);
            }
            posInDataFile += actLsmRec.getLen();
            posInIndexFile += actIndexedKey.getLen();
        }

    }

    private static ByteBuffer readFileInBuffer(File file, int bufSize) throws IOException {
        ByteBuffer bb = ByteBuffer.allocateDirect(bufSize);
        try (FileChannel channel = FileChannel.open(file.toPath(), StandardOpenOption.READ)) {
            channel.read(bb, 0);
        }
        bb.rewind();
        return bb;
    }

}