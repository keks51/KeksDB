package com.keks.kv_storage.lsm.io;

import com.keks.kv_storage.io.RafFile;
import com.keks.kv_storage.ex.KVStoreIOException;
import com.keks.kv_storage.lsm.utils.BloomFilter;

import java.io.File;
import java.io.IOException;


public class BloomFilterRafRW extends RafFile {

    public static final String BLOOM_FILTER_FILE_NAME = "bloom-filter";

    public BloomFilterRafRW(File dir) {
        super( new File(dir, BLOOM_FILTER_FILE_NAME), "rw");
    }

    public BloomFilter read() {
        try {
            double falsePositiveRate = raf.readDouble();
            int insertedElemCount = raf.readInt();
            int arrayLen = raf.readInt();

            long[] array = new long[arrayLen];
            for (int i = 0; i < arrayLen; i++) {
                array[i] = raf.readLong();
            }
            close();
            return new BloomFilter(array, falsePositiveRate, insertedElemCount);
        } catch (IOException e) {
            throw new KVStoreIOException("Cannot read BloomFilter " + file.getAbsolutePath(), e);
        }
    }

    public void write(BloomFilter filter) {
        try {
            raf.writeDouble(filter.falsePositiveRate);
            raf.writeInt(filter.getNumberOfInsertedElems());
            int len = filter.longArray.length;
            raf.writeInt(len);
            for (int i = 0; i < len; i++) {
                raf.writeLong(filter.longArray[i]);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


}
