# Example of key value database based on bplus and lsm trees

- Database supports concurrent operations: put, remove, get, range select
- Bplus tree is based on page buffer which stores data both in memory and disk.
- Lsm tree is based on memcache and sstables.
- Recovery is based on WAL and checkpoint snapshots
- Http and thrift clients
- Compared with RocksDB 8.10.0. See below

# Architecture
## database
![db_png.png](architecture%2Fdb_png.png)

## LSM
![lsm_png.png](architecture%2Flsm_png.png)

## Bplus
![bplus_png.png](architecture%2Fbplus_png.png)

## Bplus tree example

[BplusSamplesTest.java](src%2Ftest%2Fjava%2Fexamples%2FBplusSamplesTest.java)
```java

@Test
public void ex1(@TempDir Path dir) {

    try (KVStore kvStore = new KVStore(dir.toFile())) {
        // creating db
        String dbName = "bplus_test_db";
        kvStore.createDB(dbName);

        // creating table
        String tblName = "bplus_table_test";
        // bplus table properties
        Properties properties = new Properties() {{
            put(BPlusConfParamsEnum.BTREE_ORDER, 400);
        }};
        kvStore.createTable(
                dbName,
                tblName,
                TableEngineType.BPLUS.toString(),
                properties);

        // adding records
        kvStore.put(dbName, tblName, "key1", "value1".getBytes());
        kvStore.put(dbName, tblName, "key2", "value2".getBytes());
        kvStore.put(dbName, tblName, "key3", "value3".getBytes());

        // getting values
        System.out.println(new String(kvStore.get(dbName, tblName, "key1"))); // value1
        System.out.println(new String(kvStore.get(dbName, tblName, "key2"))); // value2
        System.out.println(new String(kvStore.get(dbName, tblName, "key3"))); // value3
        System.out.println(kvStore.get(dbName, tblName, "key4")); // null

        // deleting several records
        kvStore.remove(dbName, tblName, "key1");
        kvStore.remove(dbName, tblName, "key2");

        // getting records
        System.out.println(kvStore.get(dbName, tblName, "key1")); // null
        System.out.println(kvStore.get(dbName, tblName, "key2")); // null
        System.out.println(new String(kvStore.get(dbName, tblName, "key3"))); // value3
        System.out.println(kvStore.get(dbName, tblName, "key4")); // null

        // dropping table
        kvStore.dropTable(dbName, tblName);

        // dropping db
        kvStore.dropDB(dbName);
    }
}
```

## Lsm tree

[LsmSamplesTest.java](src%2Ftest%2Fjava%2Fexamples%2FLsmSamplesTest.java)

```java

@Test
public void ex1(@TempDir Path dir) {

    try (KVStore kvStore = new KVStore(dir.toFile())) {
        // creating db
        String dbName = "lsm_test_db";
        kvStore.createDB(dbName);

        // creating table
        String tblName = "lsm_table_test";
        // lsm table properties
        Properties properties = new Properties() {{
            put(LsmConfParamsEnum.MEM_CACHE_SIZE, 1_000_000);
            put(LsmConfParamsEnum.BLOOM_FILTER_FALSE_POSITIVE_RATE, 0.1);
        }};
        kvStore.createTable(
                dbName,
                tblName,
                TableEngineType.LSM.toString(),
                properties);

        // adding records
        kvStore.put(dbName, tblName, "key1", "value1".getBytes());
        kvStore.put(dbName, tblName, "key2", "value2".getBytes());
        kvStore.put(dbName, tblName, "key3", "value3".getBytes());

        // getting values
        System.out.println(new String(kvStore.get(dbName, tblName, "key1"))); // value1
        System.out.println(new String(kvStore.get(dbName, tblName, "key2"))); // value2
        System.out.println(new String(kvStore.get(dbName, tblName, "key3"))); // value3
        System.out.println(kvStore.get(dbName, tblName, "key4")); // null

        // deleting several records
        kvStore.remove(dbName, tblName, "key1");
        kvStore.remove(dbName, tblName, "key2");

        // getting records
        System.out.println(kvStore.get(dbName, tblName, "key1")); // null
        System.out.println(kvStore.get(dbName, tblName, "key2")); // null
        System.out.println(new String(kvStore.get(dbName, tblName, "key3"))); // value3
        System.out.println(kvStore.get(dbName, tblName, "key4")); // null

        // dropping table
        kvStore.dropTable(dbName, tblName);

        // dropping db
        kvStore.dropDB(dbName);
    }
}
```

# Performance benchmarks

All the benchmarks are run on MacBook Pro     
Processor: 2,4 GHz 8-Core Intel Core i9     
Memory: 32 GB 2667 MHz DDR4     
macOS: 13.3

Global Parameters:
 - JDK: corretto-1.8.0_362
 - -Xmx1g
 - Key size: 12 bytes
 - Value size: 500 bytes
 - all operations are done in parallel
 - all records are written in random order
 - WAL is disabled
 - No optimization process in background (Merging sstables, compacting bplus file on disk, ...)
 - Each test was run 10 times. Avg result is taken

## Bplus tree
Parameters:
 - Page buffer size: 256mb
 - Page buffer clean-up %: if 80% is occupied
 - Page size: 8kb
 - Tree Order: 451

[BplusPerfTest.java](src%2Ftest%2Fjava%2Fperf_test%2FBplusPerfTest.java)
### PUT (5 million records, approx 2.5G)

| threads | total sec | mean millis | ops/sec | p0.5 millis | p0.75 millis | p0.99 millis | p0.999 millis |
|---------|-----------|-------------|---------|-------------|--------------|--------------|---------------|
| 1       | 168.9     | 0.030       | 29612   | 0.045       | 0.086        | 0.205        | 0.278         |
| 4       | 149.4     | 0.110       | 33457   | 0.147       | 0.311        | 0.950        | 2.752         |
| 8       | 160.0     | 0.241       | 31241   | 0.311       | 0.655        | 2.228        | 6.815         |
| 16      | 160.1     | 0.482       | 31228   | 0.557       | 1.441        | 4.718        | 13.631        |
| 32      | 155.1     | 0.940       | 32233   | 1.048       | 3.145        | 8.388        | 31.457        |
| 50      | 147.3     | 1.392       | 33939   | 1.638       | 4.980        | 12.058       | 33.554        |
| 100     | 151.1     | 2.859       | 33090   | 3.670       | 9.961        | 33.554       | 50.331        |
| 160     | 164.4     | 4.940       | 30422   | 6.553       | 16.252       | 50.331       | 79.691        |
| 200     | 196.3     | 7.426       | 25471   | 10.485      | 24.117       | 71.303       | 109.051       |

![put_5m.png](perf_res%2Fbplus%2Fput_5m.png)

### GET (5 million records, approx 2.5G)

| threads | total sec | mean millis | ops/sec | p0.5 millis | p0.75 millis | p0.99 millis | p0.999 millis |
|---------|-----------|-------------|---------|-------------|--------------|--------------|---------------|
| 1       | 16.0      | 0.001       | 313087  | 0.001       | 0.001        | 0.002        | 0.026         |
| 4       | 10.3      | 0.003       | 484355  | 0.002       | 0.003        | 0.019        | 0.033         |
| 8       | 9.4       | 0.005       | 533560  | 0.004       | 0.004        | 0.010        | 0.026         |
| 16      | 12.0      | 0.007       | 418235  | 0.005       | 0.006        | 0.016        | 0.026         |
| 32      | 12.8      | 0.007       | 390381  | 0.005       | 0.006        | 0.017        | 0.031         |
| 50      | 13.1      | 0.007       | 381562  | 0.005       | 0.006        | 0.018        | 0.035         |
| 100     | 13.0      | 0.007       | 384408  | 0.005       | 0.006        | 0.016        | 0.033         |
| 160     | 12.7      | 0.006       | 394228  | 0.005       | 0.006        | 0.013        | 0.024         |
| 200     | 12.8      | 0.006       | 390320  | 0.004       | 0.005        | 0.012        | 0.024         |

![get_5m.png](perf_res%2Fbplus%2Fget_5m.png)

## Lsm tree

Parameters:
- MemTable size: 256mb
- Sparse index size in records: 8192 records
- All cashes are disabled

[LsmPerfTest.java](src%2Ftest%2Fjava%2Fperf_test%2FLsmPerfTest.java)
### Put (5 million records, approx 2.5G)

| threads | total sec | mean millis | ops/sec | p0.5 millis | p0.75 millis | p0.99 millis | p0.999 millis |
|---------|-----------|-------------|---------|-------------|--------------|--------------|---------------|
| 1       | 31.3      | 0.003       | 159555  | 0.002       | 0.003        | 0.006        | 0.010         |
| 4       | 27.9      | 0.005       | 179031  | 0.004       | 0.005        | 0.009        | 0.026         |
| 8       | 25.0      | 0.006       | 199696  | 0.005       | 0.006        | 0.011        | 0.033         |
| 16      | 26.0      | 0.011       | 192581  | 0.006       | 0.007        | 0.013        | 0.197         |
| 32      | 27.0      | 0.020       | 185384  | 0.006       | 0.008        | 0.020        | 0.246         |
| 50      | 28.7      | 0.027       | 174520  | 0.006       | 0.008        | 0.014        | 0.246         |
| 100     | 27.7      | 0.049       | 180642  | 0.006       | 0.007        | 0.014        | 0.328         |
| 160     | 25.9      | 0.045       | 193221  | 0.005       | 0.007        | 0.012        | 0.345         |
| 200     | 26.6      | 0.077       | 188097  | 0.006       | 0.007        | 0.021        | 0.371         |

![put_5m.png](perf_res%2Flsm%2Fput_5m.png)

### Get (1 million records, approx 500mb)

| threads | total sec | mean millis | ops/sec | p0.5 millis | p0.75 millis | p0.99 millis | p0.999 millis |
|---------|-----------|-------------|---------|-------------|--------------|--------------|---------------|
| 1       | 234.6     | 0.231       | 4261    | 0.237       | 0.344        | 0.524        | 0.786         |
| 4       | 91.8      | 0.357       | 10889   | 0.376       | 0.557        | 0.819        | 1.179         |
| 8       | 71.8      | 0.550       | 13932   | 0.557       | 0.819        | 1.572        | 6.553         |
| 16      | 72.6      | 1.109       | 13778   | 1.048       | 1.572        | 3.407        | 13.106        |
| 32      | 73.2      | 2.239       | 13668   | 1.113       | 1.637        | 46.136       | 125.828       |
| 50      | 70.7      | 3.372       | 14145   | 1.113       | 1.572        | 96.468       | 201.326       |
| 100     | 69.6      | 6.595       | 14360   | 1.113       | 1.637        | 218.103      | 419.429       |
| 160     | 69.0      | 10.406      | 14484   | 1.048       | 1.572        | 352.321      | 603.979       |
| 200     | 72.3      | 13.647      | 13825   | 1.113       | 1.572        | 452.984      | 738.196       |

![get_1m.png](perf_res%2Flsm%2Fget_1m.png)

## RocksDB (Lsm tree)

Parameters:
- MemTable size: 256mb
- Sparse index size in records: 8192 records
- All cashes are disabled
- version: 8.10.0

### Put (5 million records, approx 2.5G)
[write_test.sh](perf_res%2Frocksdb%2Fwrite_test.sh)

| threads | total sec | mean millis | ops/sec | p0.5 millis | p0.75 millis | p0.99 millis | p0.999 millis |
|---------|-----------|-------------|---------|-------------|--------------|--------------|---------------|
| 1       | 11.3      | 0.002       | 441666  | 0.002       | 0.002        | 0.005        | 0.021         |
| 4       | 6.7       | 0.005       | 744083  | 0.004       | 0.005        | 0.020        | 0.046         |
| 8       | 4.5       | 0.007       | 1120613 | 0.006       | 0.008        | 0.029        | 0.092         |
| 16      | 8.4       | 0.027       | 592084  | 0.009       | 0.014        | 0.179        | 0.505         |
| 32      | 6.7       | 0.042       | 746748  | 0.010       | 0.068        | 0.166        | 1.915         |
| 50      | 30.0      | 0.297       | 166481  | 0.287       | 0.355        | 0.799        | 4.216         |
| 100     | 44.1      | 0.880       | 113488  | 0.828       | 1.054        | 1.702        | 4.065         |
| 160     | 60.1      | 1.923       | 83160   | 1.837       | 2.346        | 3.140        | 8.487         |
| 200     | 72.4      | 2.893       | 69092   | 2.729       | 3.469        | 4.398        | 11.258        |

![put_5m.png](perf_res%2Frocksdb%2Fput_5m.png)

### Get (1 million records, approx 500mb)
[read_test.sh](perf_res%2Frocksdb%2Fread_test.sh)

| threads | total sec | mean millis | ops/sec | p0.5 millis | p0.75 millis | p0.99 millis | p0.999 millis | p0.9999 millis |
|---------|-----------|-------------|---------|-------------|--------------|--------------|---------------|----------------|
| 1       | 978.6     | 0.979       | 1021    | 0.982       | 1.160        | 3.279        | 4.348         | 6.425          |
| 4       | 538.6     | 2.153       | 1856    | 1.885       | 2.467        | 7.860        | 9.834         | 18.555         |
| 8       | 526.2     | 4.207       | 1900    | 3.910       | 4.608        | 12.487       | 20.436        | 34.563         |
| 16      | 553.2     | 8.843       | 1807    | 8.003       | 9.696        | 30.507       | 62.561        | 105.109        |
| 32      | 563.0     | 17.985      | 1776    | 13.135      | 23.148       | 72.873       | 109.830       | 170.000        |
| 50      | 448.6     | 22.372      | 2228    | 12.581      | 29.524       | 124.573      | 168.988       | 244.832        |
| 100     | 372.6     | 37.034      | 2683    | 9.892       | 39.344       | 313.190      | 453.023       | 558.513        |
| 160     | 282.2     | 44.554      | 3543    | 5.875       | 13.964       | 561.457      | 828.740       | 858.606        |
| 200     | 244.6     | 47.957      | 4087    | 4.905       | 13.880       | 636.681      | 850.862       | 1146.667       |

![get_1m.png](perf_res%2Frocksdb%2Fget_1m.png)


## Bplus tree vs Lsm tree vs RocksDB

### PUT (5 million records, approx 2.5G)

![put_op_sec_5m.png](perf_res%2Fput_op_sec_5m.png)

![put_sec_5m.png](perf_res%2Fput_sec_5m.png)

### GET (1 million records, approx 500mb)

![get_op_sec_1m.png](perf_res%2Fget_op_sec_1m.png)

![get_sec_1m.png](perf_res%2Fget_sec_1m.png)


