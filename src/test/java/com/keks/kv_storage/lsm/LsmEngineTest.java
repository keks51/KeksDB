package com.keks.kv_storage.lsm;

import com.keks.kv_storage.record.KVRecord;
import com.keks.kv_storage.lsm.conf.LsmConf;
import com.keks.kv_storage.query.Query;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import utils.TestScheduler;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static utils.TestUtils.assertNumberOfSSTables;
import static utils.TestUtils.assertSStablesExists;


class LsmEngineTest implements TestScheduler {

    private final Query queryAll = new Query.QueryBuilder().withNoMinBound().withNoMaxBound().withNoLimit().build();
    @Test
    public void createCloseRead(@TempDir Path tmpPath) throws IOException {
        String kvTableName = "test1";
        File tableDir = tmpPath.resolve(kvTableName).toFile();
        tableDir.mkdir();
        {
            LsmConf lsmConf = new LsmConf(3, 3, 0.5);
            LsmEngine lsmEngine = LsmEngine.createNewTable("", tableDir, lsmConf, scheduler);
            lsmEngine.put("key10", "10");
            lsmEngine.close();
        }

        {

            LsmEngine lsmEngine = LsmEngine.loadTable("", tableDir, scheduler);
            assertEquals("key10", lsmEngine.get("key10").key);
            assertEquals(1, lsmEngine.getRecordsCnt(queryAll));
            lsmEngine.close();
        }
    }

    @Test
    public void testPut(@TempDir Path tmpPath) throws IOException {
        String kvTableName = "test1";
        File tableDir = tmpPath.resolve(kvTableName).toFile();
        tableDir.mkdir();
        LsmConf lsmConf = new LsmConf(3, 4, 0.5);
        LsmEngine lsmEngine = LsmEngine.createNewTable("", tableDir, lsmConf, scheduler);
        lsmEngine.put("key10", "10");
        lsmEngine.put("key11", "11");
        lsmEngine.put("key12", "12");

        assertSStablesExists(tableDir);
        assertEquals("10", new String(lsmEngine.get("key10").valueBytes));
        assertEquals("11", new String(lsmEngine.get("key11").valueBytes));
        assertEquals("12", new String(lsmEngine.get("key12").valueBytes));
        assertEquals(3, lsmEngine.getRecordsCnt(queryAll));
    }

    @Test
    public void testDelete(@TempDir Path tmpPath) throws IOException {
        String kvTableName = "test1";
        File tableDir = tmpPath.resolve(kvTableName).toFile();
        tableDir.mkdir();
        LsmConf lsmConf = new LsmConf(3, 20, 0.5);
        LsmEngine lsmEngine = LsmEngine.createNewTable("", tableDir, lsmConf, scheduler);
        lsmEngine.remove("key10");
        lsmEngine.remove("key11");
        lsmEngine.remove("key12");
        lsmEngine.remove("key13");

        lsmEngine.put("key10", "10");
        lsmEngine.put("key11", "11");
        lsmEngine.put("key12", "12");

        lsmEngine.remove("key10");
        lsmEngine.remove("key11");
        lsmEngine.remove("key12");
        lsmEngine.remove("key13");

        assertNumberOfSSTables(tableDir, 0);
        assertNull(lsmEngine.get("key9"));
        assertNull(lsmEngine.get("key10"));
        assertNull(lsmEngine.get("key11"));
        assertNull(lsmEngine.get("key12"));
        assertNull(lsmEngine.get("key13"));
        assertEquals(0, lsmEngine.getRecordsCnt(queryAll));
    }

    @Test
    public void testFlushGetDelete(@TempDir Path tmpPath) throws IOException, InterruptedException {
        String kvTableName = "test1";
        File tableDir = tmpPath.resolve(kvTableName).toFile();
        tableDir.mkdir();
        LsmConf lsmConf = new LsmConf(3, 4, 0.5, true);
        LsmEngine lsmEngine = LsmEngine.createNewTable("", tableDir, lsmConf, scheduler);

        lsmEngine.remove("key10"); // 1 record in memory
        lsmEngine.remove("key11"); // 2 records in memory
        lsmEngine.remove("key12"); // 3 records in memory
        lsmEngine.remove("key13"); // 4 records in memory
        lsmEngine.remove("key13"); // 4 records in memory

        lsmEngine.put("key10", "10"); // 4 records in memory
        lsmEngine.put("key11", "11"); // 4 records in memory
        lsmEngine.put("key12", "12"); // 4 records in memory

        lsmEngine.remove("key10"); // 4 records in memory
        lsmEngine.remove("key11"); // 4 records in memory
        lsmEngine.remove("key12"); // 4 records in memory
        lsmEngine.remove("key13"); // 4 records in memory

        lsmEngine.put("key9", "9"); // internal flush. 0 records in memory and 1 file on disk
        lsmEngine.put("key11", "11"); // 2 records in memory
        lsmEngine.put("key15", "15"); // 3 records in memory

        assertSStablesExists(tableDir, "1v1");

        assertEquals("9", new String(lsmEngine.get("key9").valueBytes));
        assertNull(lsmEngine.get("key10"));
        assertEquals("11", new String(lsmEngine.get("key11").valueBytes));
        assertNull(lsmEngine.get("key12"));
        assertNull(lsmEngine.get("key13"));

        lsmEngine.forceFlush(); // 0 records in memory and 2 files on disk
        assertSStablesExists(tableDir, "1v1", "2v1");
        assertEquals("9", new String(lsmEngine.get("key9").valueBytes));
        assertNull(lsmEngine.get("key10"));
        assertEquals("11", new String(lsmEngine.get("key11").valueBytes));
        assertNull(lsmEngine.get("key12"));
        assertNull(lsmEngine.get("key13"));


        assertEquals(3, lsmEngine.getRecordsCnt(queryAll));
    }

    @Test
    public void testCompaction(@TempDir Path tmpPath) throws IOException {
        String kvTableName = "test1";
        File tableDir = tmpPath.resolve(kvTableName).toFile();
        tableDir.mkdir();
        LsmConf lsmConf = new LsmConf(3, 4, 0.5, true);
        LsmEngine lsmEngine = LsmEngine.createNewTable("", tableDir, lsmConf, scheduler);

        lsmEngine.remove("key10"); // 1 record in memory
        lsmEngine.remove("key11"); // 2 records in memory
        lsmEngine.remove("key12"); // 3 records in memory
        lsmEngine.remove("key13"); // 4 records in memory

        lsmEngine.put("key10", "10"); // 4 records in memory
        lsmEngine.put("key11", "11"); // 4 records in memory
        lsmEngine.put("key12", "12"); // 4 records in memory

        lsmEngine.remove("key10"); // 4 records in memory
        lsmEngine.remove("key11"); // 4 records in memory
        lsmEngine.remove("key12"); // 4 records in memory
        lsmEngine.remove("key13"); // 5 records in memory

        lsmEngine.put("key9", "9"); // internal flush. 0 records in memory and 1 file on disk
        lsmEngine.put("key11", "11"); // 2 records in memory
        lsmEngine.put("key15", "15"); // 3 records in memory

        lsmEngine.forceFlush(); // 0 records in memory and 2 files on disk
        assertSStablesExists(tableDir, "1v1", "2v1");

        lsmEngine.optimize(); // 0 records in memory and 1 file on disk

        assertNumberOfSSTables(tableDir, 1);
        assertSStablesExists(tableDir, "2v2");
        assertEquals(new ArrayList<>() {{
                         add(new KVRecord("key11", "11"));
                         add(new KVRecord("key15", "15"));
                         add(new KVRecord("key9", "9"));
                     }},
                lsmEngine.getAll().getAsArr());

        assertEquals(3, lsmEngine.getRecordsCnt(queryAll));
    }


    @RepeatedTest(10)
    public void testLsmEngine1(@TempDir Path tmpPath) throws IOException {
        LsmConf lsmConf = new LsmConf(10, 50, 0.90, false, 10, false, 0, 1);
        Path tablePath = tmpPath.resolve("test_table");
        Files.createDirectory(tablePath);
        testLsmEngine(tablePath.toFile(), lsmConf);
    }

    @RepeatedTest(10)
    public void testLsmEngine2(@TempDir Path tmpPath) throws IOException {
        LsmConf lsmConf = new LsmConf(10, 50, 0.90, true, 10, false, 0, 1);
        Path tablePath = tmpPath.resolve("test_table");
        Files.createDirectory(tablePath);
        testLsmEngine(tablePath.toFile(), lsmConf);
    }

    @RepeatedTest(10)
    public void testLsmEngine3(@TempDir Path tmpPath) throws IOException {
        LsmConf lsmConf = new LsmConf(10, 50, 0.90, false, 10, true, 0, 1);
        Path tablePath = tmpPath.resolve("test_table");
        Files.createDirectory(tablePath);
        testLsmEngine(tablePath.toFile(), lsmConf);
    }

    @RepeatedTest(10)
    public void testLsmEngine4(@TempDir Path tmpPath) throws IOException {
        LsmConf lsmConf = new LsmConf(10, 50, 0.90, true, 10, true, 0, 1);
        Path tablePath = tmpPath.resolve("test_table");
        Files.createDirectory(tablePath);
        testLsmEngine(tablePath.toFile(), lsmConf);
    }

    @RepeatedTest(10)
    public void testLsmEngine5(@TempDir Path tmpPath) throws IOException {
        LsmConf lsmConf = new LsmConf(2, 5, 0.90, true, 10, true, 0, 1);
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
            } else {
                assertEquals(value, new String(lsmEngine.get(key).valueBytes));
                expRecords.add(new KVRecord(key, value));
            }
        }

        for (int i = 0; i < taskCnt; i++) {
            String key = "key" + String.format("%07d", i);;
            String value = "value" + i;
            if (i % 3 == 0) {
                assertNull(lsmEngine.get(key));
            } else {
                assertEquals(value, new String(lsmEngine.get(key).valueBytes));
            }
        }


        assertEquals(expRecords,
                lsmEngine.getRangeRecords(new Query.QueryBuilder().build()).getAsArr());

        lsmEngine.close();
    }

}