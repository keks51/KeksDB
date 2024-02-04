package com.keks.kv_storage.lsm.ss_table;

import com.keks.kv_storage.lsm.io.SSTableReader;
import com.keks.kv_storage.record.KVRecord;
import com.keks.kv_storage.lsm.conf.LsmConf;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import utils.TestUtils;
import utils.TestScheduler;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static utils.TestUtils.assertSStablesExists;


class SSTablesManagerTest implements TestScheduler {

    //  TODO ssTablesSortedList.size() was removed. think how to implement size or remove this test
//    @Test
//    public void testCreateSSTableOnDisk(@TempDir Path tmpPath) throws IOException {
//        LsmConf lsmConf = new LsmConf(5, 5, 0.5);
//        SSTablesManager ssTablesManager = new SSTablesManager("", tmpPath.toFile(), lsmConf, scheduler);
//        Iterator<KVRecord> map1 = generateData(11, 23);
//        ssTablesManager.createSSTableOnDisk(map1, 100);
//
//        Iterator<KVRecord> map2 = generateData(25, 29);
//        ssTablesManager.createSSTableOnDisk(map2, 100);
//
//        Iterator<KVRecord> map3 = generateData(32, 38);
//        ssTablesManager.createSSTableOnDisk(map3, 100);
//
////        assertEquals(3, ssTablesManager.ssTablesSortedList.size());
//    }

    @Test
    public void testMergeAllTables0(@TempDir Path tmpPath) throws IOException {
        LsmConf lsmConf = new LsmConf(5, 100, 0.5);
        SSTablesManager ssTablesManager = new SSTablesManager("", tmpPath.toFile(), lsmConf, scheduler);

        ssTablesManager.createSSTableOnDisk(new ArrayList<KVRecord>() {{
            add(new KVRecord("key1", "1"));
            add(new KVRecord("key2", "2"));
            add(new KVRecord("key3", "3"));
        }}.iterator(), 3);

        ssTablesManager.createSSTableOnDisk(new ArrayList<KVRecord>() {{
            add(new KVRecord("key1"));
            add(new KVRecord("key2"));
            add(new KVRecord("key3"));
        }}.iterator(), 3);

        TestUtils.assertNumberOfSSTables(tmpPath.toFile(), 2);
        ssTablesManager.mergeAllTables();
        TestUtils.assertNumberOfSSTables(tmpPath.toFile(), 1);

        SSTable v2v = SSTablesManager.readSSTable(tmpPath.resolve("2v2").toFile(), lsmConf);
        assertEquals(v2v.inMemorySparseIndexes.size(), 0);
    }

    @Test
    public void testMergeAllTables1(@TempDir Path tmpPath) throws IOException {
        LsmConf lsmConf = new LsmConf(5, 100, 0.5);
        SSTablesManager ssTablesManager = new SSTablesManager("", tmpPath.toFile(), lsmConf, scheduler);
        Iterator<KVRecord> map1 = generateData(11, 23);
        ssTablesManager.createSSTableOnDisk(map1, 100);

        Iterator<KVRecord> map2 = generateData(25, 29);
        ssTablesManager.createSSTableOnDisk(map2, 100);

        Iterator<KVRecord> map3 = generateData(32, 38);
        ssTablesManager.createSSTableOnDisk(map3, 100);

        TestUtils.assertNumberOfSSTables(tmpPath.toFile(), 3);
        ssTablesManager.mergeAllTables();
        TestUtils.assertNumberOfSSTables(tmpPath.toFile(), 1);
    }

    @Test
    public void testMergeAllTables2(@TempDir Path tmpPath) throws IOException {
        LsmConf lsmConf = new LsmConf(5, 100, 0.5);
        SSTablesManager ssTablesManager = new SSTablesManager("", tmpPath.toFile(), lsmConf, scheduler);

        ssTablesManager.createSSTableOnDisk(generateData(11, 23), 100);
        ssTablesManager.createSSTableOnDisk(generateData(25, 29), 100);
        ssTablesManager.createSSTableOnDisk(generateData(32, 38), 100);

        ssTablesManager.createSSTableOnDisk(generateDeleteData(11, 23), 100);
        ssTablesManager.createSSTableOnDisk(generateDeleteData(25, 29), 100);
        ssTablesManager.createSSTableOnDisk(generateDeleteData(32, 38), 100);


        TestUtils.assertNumberOfSSTables(tmpPath.toFile(), 6);
        ssTablesManager.mergeAllTables();
        TestUtils.assertNumberOfSSTables(tmpPath.toFile(), 1);
    }

    @Test
    public void testMergeNLastSSTables1(@TempDir Path tmpPath) throws IOException {
        LsmConf lsmConf = new LsmConf(5, 100, 0.5);
        SSTablesManager ssTablesManager = new SSTablesManager("", tmpPath.toFile(), lsmConf, scheduler);

        {
            ssTablesManager.createSSTableOnDisk(generateData(11, 23), 100);
            ssTablesManager.createSSTableOnDisk(generateData(25, 29), 100);
            ssTablesManager.createSSTableOnDisk(generateData(32, 38), 100);

            ssTablesManager.createSSTableOnDisk(generateDeleteData(11, 23), 100);
            ssTablesManager.createSSTableOnDisk(generateDeleteData(25, 29), 100);
            ssTablesManager.createSSTableOnDisk(generateDeleteData(32, 38), 100);


            TestUtils.assertNumberOfSSTables(tmpPath.toFile(), 6);

            ssTablesManager.mergeNLastSSTables(2);
            TestUtils.assertNumberOfSSTables(tmpPath.toFile(), 5);
            assertSStablesExists(tmpPath.toFile(), "2v2", "3v1", "4v1", "5v1", "6v1");

            ssTablesManager.mergeNLastSSTables(2);
            TestUtils.assertNumberOfSSTables(tmpPath.toFile(), 4);
            assertSStablesExists(tmpPath.toFile(), "3v2", "4v1", "5v1", "6v1");

            ssTablesManager.mergeNLastSSTables(2);
            TestUtils.assertNumberOfSSTables(tmpPath.toFile(), 3);
            assertSStablesExists(tmpPath.toFile(), "4v2", "5v1", "6v1");

            ssTablesManager.mergeNLastSSTables(2);
            TestUtils.assertNumberOfSSTables(tmpPath.toFile(), 2);
            assertSStablesExists(tmpPath.toFile(), "5v2", "6v1");

            ssTablesManager.mergeNLastSSTables(2);
            TestUtils.assertNumberOfSSTables(tmpPath.toFile(), 1);
            assertSStablesExists(tmpPath.toFile(), "6v2");
        }

        {
            ssTablesManager.createSSTableOnDisk(generateData(11, 23), 100);
            ssTablesManager.createSSTableOnDisk(generateData(25, 29), 100);
            ssTablesManager.createSSTableOnDisk(generateData(32, 38), 100);

            ssTablesManager.createSSTableOnDisk(generateDeleteData(11, 23), 100);
            ssTablesManager.createSSTableOnDisk(generateDeleteData(25, 29), 100);
            ssTablesManager.createSSTableOnDisk(generateDeleteData(32, 38), 100);


            TestUtils.assertNumberOfSSTables(tmpPath.toFile(), 7);

            ssTablesManager.mergeNLastSSTables(2);
            TestUtils.assertNumberOfSSTables(tmpPath.toFile(), 6);

            ssTablesManager.mergeNLastSSTables(2);
            TestUtils.assertNumberOfSSTables(tmpPath.toFile(), 5);

            ssTablesManager.mergeNLastSSTables(2);
            TestUtils.assertNumberOfSSTables(tmpPath.toFile(), 4);

            ssTablesManager.mergeNLastSSTables(2);
            TestUtils.assertNumberOfSSTables(tmpPath.toFile(), 3);

            ssTablesManager.mergeNLastSSTables(2);
            TestUtils.assertNumberOfSSTables(tmpPath.toFile(), 2);

            ssTablesManager.mergeNLastSSTables(2);
            TestUtils.assertNumberOfSSTables(tmpPath.toFile(), 1);
        }

        {
            ssTablesManager.createSSTableOnDisk(generateData(11, 23), 100);
            ssTablesManager.createSSTableOnDisk(generateData(25, 29), 100);
            ssTablesManager.createSSTableOnDisk(generateData(32, 38), 100);

            ssTablesManager.createSSTableOnDisk(generateDeleteData(11, 23), 100);
            ssTablesManager.createSSTableOnDisk(generateDeleteData(25, 29), 100);
            ssTablesManager.createSSTableOnDisk(generateDeleteData(32, 38), 100);


            TestUtils.assertNumberOfSSTables(tmpPath.toFile(), 7);

            ssTablesManager.mergeNLastSSTables(2);
            TestUtils.assertNumberOfSSTables(tmpPath.toFile(), 6);

            ssTablesManager.mergeNLastSSTables(2);
            TestUtils.assertNumberOfSSTables(tmpPath.toFile(), 5);

            ssTablesManager.mergeNLastSSTables(2);
            TestUtils.assertNumberOfSSTables(tmpPath.toFile(), 4);

            ssTablesManager.mergeNLastSSTables(2);
            TestUtils.assertNumberOfSSTables(tmpPath.toFile(), 3);

            ssTablesManager.mergeNLastSSTables(2);
            TestUtils.assertNumberOfSSTables(tmpPath.toFile(), 2);

            ssTablesManager.mergeNLastSSTables(2);
            TestUtils.assertNumberOfSSTables(tmpPath.toFile(), 1);
        }
    }

    @Test
    public void test1244(@TempDir Path tmpPath) throws IOException {
        ReentrantReadWriteLock.ReadLock mockedLock = new ReentrantReadWriteLock.ReadLock(new ReentrantReadWriteLock()) {
            @Override
            public void unlock() {}
        };

        LsmConf lsmConf = new LsmConf(5, 100, 0.5);
        SSTablesManager ssTablesManager = new SSTablesManager("", tmpPath.toFile(), lsmConf, scheduler);

        ssTablesManager.createSSTableOnDisk(new ArrayList<KVRecord>() {{
            add(new KVRecord("key1", "1"));
            add(new KVRecord("key2", "2"));
            add(new KVRecord("key3", "3"));
        }}.iterator(), 3);

        ssTablesManager.createSSTableOnDisk(new ArrayList<KVRecord>() {{
            add(new KVRecord("key4", "4"));
            add(new KVRecord("key5", "5"));
            add(new KVRecord("key6", "6"));
        }}.iterator(), 3);

        System.out.println(ssTablesManager.searchKey("key1"));
        System.out.println(ssTablesManager.searchKey("key2"));
        System.out.println(ssTablesManager.searchKey("key3"));
        System.out.println(ssTablesManager.searchKey("key4"));
        System.out.println(ssTablesManager.searchKey("key5"));
        System.out.println(ssTablesManager.searchKey("key6"));

        ssTablesManager.mergeNLastSSTables(2);

        System.out.println(ssTablesManager.searchKey("key1"));
        System.out.println(ssTablesManager.searchKey("key2"));
        System.out.println(ssTablesManager.searchKey("key3"));
        System.out.println(ssTablesManager.searchKey("key4"));
        System.out.println(ssTablesManager.searchKey("key5"));
        System.out.println(ssTablesManager.searchKey("key6"));

//        LsmConf lsmConf = new LsmConf(3, 5, 0.5);


//        LsmEngine lsmEngine = LsmEngine.createNewTable("", tableDir, lsmConf, scheduler);
//
//        lsmEngine.remove("key10"); // 1 record in memory
//        lsmEngine.remove("key11"); // 2 records in memory
//        lsmEngine.remove("key12"); // 3 records in memory
//        lsmEngine.remove("key13"); // 4 records in memory
//
//        lsmEngine.put("key10", "10"); // 4 records in memory
//        lsmEngine.put("key11", "11"); // 4 records in memory
//        lsmEngine.put("key12", "12"); // 4 records in memory
//
//        lsmEngine.remove("key10"); // 4 records in memory
//        lsmEngine.remove("key11"); // 4 records in memory
//        lsmEngine.remove("key12"); // 4 records in memory
//        lsmEngine.remove("key13"); // 5 records in memory
//
//        lsmEngine.put("key9", "9"); // internal flush. 0 records in memory and 1 file on disk
//        lsmEngine.put("key11", "11"); // 2 records in memory
//        lsmEngine.put("key15", "15"); // 3 records in memory
    }


    public static Iterator<KVRecord> generateData(int startOffset, int endOffset) {
        SortedMap<String, String> map = new TreeMap<>();
        for (int i = startOffset; i <= endOffset; i++) {
            String key = "key" + i;
            String value = "age-" + i;
            map.put(key, value);
        }
        return map.entrySet().stream().map(e -> new KVRecord(e.getKey(), e.getValue())).iterator();
    }

    public static Iterator<KVRecord> generateDeleteData(int startOffset, int endOffset) {
        SortedMap<String, KVRecord> map = new TreeMap<>();
        for (int i = startOffset; i <= endOffset; i++) {
            String key = "key" + i;
            String value = "DELETED";
            map.put(key, new KVRecord(key));
        }
//        return map.entrySet().stream().map(e -> new KVRecord(e.getKey(), e.getValue())).iterator();
        return map.values().iterator();
    }

}