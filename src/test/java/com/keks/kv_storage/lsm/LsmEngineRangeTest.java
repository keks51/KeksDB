package com.keks.kv_storage.lsm;

import com.keks.kv_storage.record.KVRecord;
import com.keks.kv_storage.lsm.conf.LsmConf;
import com.keks.kv_storage.query.range.MaxRangeKey;
import com.keks.kv_storage.query.range.MinRangeKey;
import com.keks.kv_storage.query.range.RangeSearchKey;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.io.TempDir;
import utils.TestScheduler;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;


class LsmEngineRangeTest implements TestScheduler {
    @RepeatedTest(5)
    public void testLsmEngine1(@TempDir Path tmpPath) throws IOException {
        LsmConf lsmConf = new LsmConf(10, 500, 0.90, false, 2, false, 0, 1);
        Path tablePath = tmpPath.resolve("test_table");
        Files.createDirectory(tablePath);
        testLsmEngine(tablePath.toFile(), lsmConf);
    }

    @RepeatedTest(5)
    public void testLsmEngine2(@TempDir Path tmpPath) throws IOException {
        LsmConf lsmConf = new LsmConf(10, 500, 0.90, true, 2, false, 0, 1);
        Path tablePath = tmpPath.resolve("test_table");
        Files.createDirectory(tablePath);
        testLsmEngine(tablePath.toFile(), lsmConf);
    }

    @RepeatedTest(5)
    public void testLsmEngine3(@TempDir Path tmpPath) throws IOException {
        LsmConf lsmConf = new LsmConf(10, 500, 0.90, false, 2, true, 0, 1);
        Path tablePath = tmpPath.resolve("test_table");
        Files.createDirectory(tablePath);
        testLsmEngine(tablePath.toFile(), lsmConf);
    }

    @RepeatedTest(5)
    public void testLsmEngine4(@TempDir Path tmpPath) throws IOException {
        LsmConf lsmConf = new LsmConf(10, 500, 0.90, true, 2, true, 0, 1);
        Path tablePath = tmpPath.resolve("test_table");
        Files.createDirectory(tablePath);
        testLsmEngine(tablePath.toFile(), lsmConf);
    }

    private static void testLsmEngine(File tableDir, LsmConf lsmConf) throws IOException {

        ArrayList<KVRecord> expRecords = new ArrayList<>();
        LsmEngine lsmEngine = LsmEngine.createNewTable("", tableDir, lsmConf, scheduler);

        int taskCnt = 10_000;
        for (int i = 0; i < taskCnt; i++) {
            String key = "key" + String.format("%07d", i);;
            String value = "value" + i;

            lsmEngine.put(key, value);

            assertEquals(value, new String(lsmEngine.get(key).valueBytes));
            if (i % 3 == 0) {
                lsmEngine.remove(key);
                assertNull(lsmEngine.get(key));
                assertEquals(expRecords,
                        lsmEngine.getRangeRecords(new MinRangeKey(), new RangeSearchKey(key, false)).getAsArr());
            } else {
                assertEquals(value, new String(lsmEngine.get(key).valueBytes));
                expRecords.add(new KVRecord(key, value));
                assertEquals(expRecords,
                        lsmEngine.getRangeRecords(new MinRangeKey(), new RangeSearchKey(key, true)).getAsArr());
            }

            assertEquals(expRecords,
                    lsmEngine.getRangeRecords(new MinRangeKey(), new MaxRangeKey()).getAsArr());

        }

        lsmEngine.optimize();
        for (int i = 0; i < taskCnt; i++) {
            String key = "key" + String.format("%07d", i);;
            String value = "value" + i;
            if (i % 3 == 0) {
                assertNull(lsmEngine.get(key));
            } else {
                assertEquals(value, new String(lsmEngine.get(key).valueBytes));
                int i1 = expRecords.indexOf(lsmEngine.get(key));
                assertEquals(expRecords.subList(0, i1 + 1),
                        lsmEngine.getRangeRecords(new MinRangeKey(), new RangeSearchKey(key, true)).getAsArr());
            }

        }


        assertEquals(expRecords,
                lsmEngine.getRangeRecords(new MinRangeKey(), new MaxRangeKey()).getAsArr());

        lsmEngine.close();
    }

}