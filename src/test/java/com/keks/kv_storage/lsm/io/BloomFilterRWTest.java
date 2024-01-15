package com.keks.kv_storage.lsm.io;

import com.keks.kv_storage.lsm.utils.BloomFilter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;


class BloomFilterRWTest {

    @Test
    public void testReadWriteBloomFilter(@TempDir Path tmpPath) {
        BloomFilter expBloomFilter = new BloomFilter(100, 0.5);
        expBloomFilter.add("keks");
        try(BloomFilterRafRW bloomFilterRafRW = new BloomFilterRafRW(tmpPath.toFile())) {
            bloomFilterRafRW.write(expBloomFilter);
        }

        try(BloomFilterRafRW bloomFilterRafRW = new BloomFilterRafRW(tmpPath.toFile())) {
            BloomFilter resBloomFilter = bloomFilterRafRW.read();
            assertEquals(expBloomFilter.falsePositiveRate, resBloomFilter.falsePositiveRate);
            assertEquals(expBloomFilter.getNumberOfInsertedElems(), resBloomFilter.getNumberOfInsertedElems());
            assertArrayEquals(expBloomFilter.longArray, resBloomFilter.longArray);
        }
    }

}
