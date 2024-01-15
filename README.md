# Example of key value database based on bplus and lsm trees
- Database supports concurrent operations: put, remove, get, range select
- Bplus tree is based on page buffer which stores data both in memory and disk.
- Lsm tree is based on memcache and sstables.
- Recovery is based on commit log and checkpoint snapshots
- Http and thrift clients

## Bplus tree example
see samples.BplusSamplesTest
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
see samples.LsmSamplesTest
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
## Bplus tree
### PUT
| threads | records | total sec | mean millis | ops/sec   | p0.1 millis | p0.5 millis | p0.75 millis | p0.99 millis | p0.999 millis | p0.9999 millis |
|---------|---------|-----------|-------------|-----------|-------------|-------------|--------------|--------------|---------------|----------------|
| 1       | 1000000 | 56        | 0.053       | 17857.143 | 0.041       | 0.043       | 0.045        | 0.162        | 0.227         | 0.358          |
| 5       | 1000000 | 14        | 0.064       | 71428.571 | 0.047       | 0.055       | 0.063        | 0.162        | 0.276         | 2.030          |
| 10      | 1000000 | 13        | 0.116       | 76923.077 | 0.068       | 0.104       | 0.137        | 0.260        | 0.424         | 6.814          |
| 15      | 1000000 | 14        | 0.181       | 71428.571 | 0.076       | 0.178       | 0.236        | 0.424        | 0.653         | 9.435          |
| 20      | 1000000 | 16        | 0.284       | 62500.000 | 0.080       | 0.309       | 0.408        | 0.621        | 0.948         | 8.911          |
| 25      | 1000000 | 17        | 0.375       | 58823.529 | 0.084       | 0.440       | 0.588        | 0.817        | 1.505         | 8.911          |
| 30      | 1000000 | 16        | 0.433       | 62500.000 | 0.084       | 0.555       | 0.686        | 0.948        | 1.374         | 14.678         |
| 50      | 1000000 | 16        | 0.745       | 62500.000 | 0.080       | 1.112       | 1.243        | 1.571        | 2.226         | 54.524         |
| 100     | 1000000 | 17        | 1.596       | 58823.529 | 0.076       | 2.619       | 2.750        | 3.406        | 13.105        | 39.844         |
| 150     | 1000000 | 19        | 2.630       | 52631.579 | 0.076       | 4.192       | 4.717        | 6.027        | 13.105        | 176.159        |
| 200     | 1000000 | 21        | 3.896       | 47619.048 | 0.080       | 6.552       | 7.076        | 8.124        | 24.115        | 184.547        | 

![bplus_put.png](perf_res%2Fbplus%2Fbplus_put.png)  


### GET

| threads | records | total sec | mean millis | ops/sec    | p0.1 millis | p0.5 millis | p0.75 millis | p0.99 millis | p0.999 millis | p0.9999 millis |
|---------|---------|-----------|-------------|------------|-------------|-------------|--------------|--------------|---------------|----------------|
| 1       | 1000000 | 3         | 0.002       | 333333.333 | 0.001       | 0.002       | 0.002        | 0.011        | 0.037         | 0.098          |
| 5       | 1000000 | 3         | 0.012       | 333333.333 | 0.004       | 0.007       | 0.012        | 0.053        | 0.131         | 0.475          |
| 10      | 1000000 | 2         | 0.009       | 500000.000 | 0.004       | 0.006       | 0.008        | 0.030        | 0.070         | 0.139          |
| 15      | 1000000 | 2         | 0.024       | 500000.000 | 0.011       | 0.018       | 0.022        | 0.074        | 0.311         | 1.049          |
| 20      | 1000000 | 2         | 0.025       | 500000.000 | 0.011       | 0.019       | 0.025        | 0.119        | 0.360         | 0.688          |
| 25      | 1000000 | 2         | 0.020       | 500000.000 | 0.006       | 0.013       | 0.021        | 0.057        | 0.172         | 0.377          |
| 30      | 1000000 | 2         | 0.024       | 500000.000 | 0.009       | 0.017       | 0.022        | 0.078        | 0.311         | 1.016          |
| 50      | 1000000 | 2         | 0.022       | 500000.000 | 0.010       | 0.019       | 0.025        | 0.059        | 0.172         | 0.328          |
| 100     | 1000000 | 2         | 0.021       | 500000.000 | 0.009       | 0.018       | 0.025        | 0.070        | 0.254         | 0.721          |
| 150     | 1000000 | 2         | 0.023       | 500000.000 | 0.009       | 0.019       | 0.025        | 0.063        | 0.164         | 0.328          |
| 200     | 1000000 | 2         | 0.021       | 500000.000 | 0.007       | 0.018       | 0.025        | 0.070        | 0.205         | 0.557          |

![bplus_get.png](perf_res%2Fbplus%2Fbplus_get.png)

### REMOVE

| threads | records | total sec | mean millis | ops/sec   | p0.1 millis | p0.5 millis | p0.75 millis | p0.99 millis | p0.999 millis | p0.9999 millis |
|---------|---------|-----------|-------------|-----------|-------------|-------------|--------------|--------------|---------------|----------------|
| 1       | 1000000 | 71        | 0.055       | 14084.507 | 0.041       | 0.043       | 0.047        | 0.162        | 0.219         | 0.408          |
| 5       | 1000000 | 28        | 0.070       | 35714.286 | 0.047       | 0.059       | 0.076        | 0.170        | 0.276         | 1.440          |
| 10      | 1000000 | 33        | 0.163       | 30303.030 | 0.068       | 0.137       | 0.219        | 0.473        | 0.653         | 2.488          |
| 15      | 1000000 | 33        | 0.246       | 30303.030 | 0.068       | 0.244       | 0.358        | 0.752        | 1.047         | 9.959          |
| 20      | 1000000 | 33        | 0.330       | 30303.030 | 0.068       | 0.375       | 0.522        | 0.850        | 1.178         | 12.057         |
| 25      | 1000000 | 33        | 0.422       | 30303.030 | 0.072       | 0.506       | 0.686        | 1.178        | 1.571         | 15.202         |
| 30      | 1000000 | 34        | 0.517       | 29411.765 | 0.072       | 0.653       | 0.850        | 1.309        | 1.833         | 27.261         |
| 50      | 1000000 | 35        | 0.910       | 28571.429 | 0.072       | 1.243       | 1.571        | 2.226        | 3.537         | 50.330         |
| 100     | 1000000 | 36        | 1.912       | 27777.778 | 0.072       | 2.882       | 3.275        | 4.061        | 15.202        | 56.621         |
| 150     | 1000000 | 37        | 3.094       | 27027.027 | 0.076       | 4.979       | 5.241        | 6.289        | 20.969        | 150.993        |
| 200     | 1000000 | 39        | 4.461       | 25641.026 | 0.080       | 7.076       | 7.338        | 8.911        | 20.969        | 218.102        |

![bplus_remove.png](perf_res%2Fbplus%2Fbplus_remove.png)


## Lsm tree
### Put
| threads | records | total sec | mean millis | ops/sec   | p0.5 millis | p0.75 millis | p0.99 millis | p0.999 millis | p0.9999 millis |
|---------|---------|-----------|-------------|-----------|-------------|--------------|--------------|---------------|----------------|
| 1       | 5000000 | 269       | 0.051       | 18587.361 | 0.042       | 0.042        | 0.146        | 0.196         | 0.253          |
| 5       | 5000000 | 99        | 0.092       | 50505.051 | 0.057       | 0.100        | 0.276        | 0.391         | 1.309          |
| 10      | 5000000 | 84        | 0.157       | 59523.810 | 0.104       | 0.178        | 0.621        | 0.883         | 2.226          |
| 15      | 5000000 | 79        | 0.221       | 63291.139 | 0.129       | 0.276        | 0.948        | 1.374         | 3.013          |
| 20      | 5000000 | 79        | 0.295       | 63291.139 | 0.170       | 0.375        | 1.374        | 2.030         | 19.921         |
| 25      | 5000000 | 77        | 0.358       | 64935.065 | 0.203       | 0.473        | 1.767        | 2.619         | 7.076          |
| 30      | 5000000 | 77        | 0.431       | 64935.065 | 0.236       | 0.588        | 2.226        | 3.275         | 26.212         |
| 50      | 5000000 | 77        | 0.728       | 64935.065 | 0.375       | 0.981        | 3.799        | 5.765         | 75.495         |
| 100     | 5000000 | 77        | 1.447       | 64935.065 | 0.719       | 1.964        | 8.911        | 14.678        | 79.690         |
| 150     | 5000000 | 78        | 2.185       | 64102.564 | 0.948       | 2.882        | 13.629       | 23.067        | 100.661        |
| 200     | 5000000 | 92        | 3.491       | 54347.826 | 1.178       | 4.061        | 25.164       | 56.621        | 142.604        |

![lsm_put.png](perf_res%2Flsm%2Flsm_put.png)

### Get
| threads | records | total sec | mean millis | ops/sec    | p0.5 millis | p0.75 millis | p0.99 millis | p0.999 millis | p0.9999 millis |
|---------|---------|-----------|-------------|------------|-------------|--------------|--------------|---------------|----------------|
| 1       | 5000000 | 69        | 0.013       | 72463.768  | 0.011       | 0.013        | 0.037        | 0.074         | 0.139          |
| 5       | 5000000 | 28        | 0.024       | 178571.429 | 0.019       | 0.025        | 0.049        | 0.086         | 0.164          |
| 10      | 5000000 | 38        | 0.067       | 131578.947 | 0.055       | 0.077        | 0.155        | 0.212         | 41.943         |
| 15      | 5000000 | 51        | 0.137       | 98039.216  | 0.118       | 0.163        | 0.360        | 0.491         | 46.137         |
| 20      | 5000000 | 62        | 0.228       | 80645.161  | 0.196       | 0.294        | 0.688        | 0.983         | 44.040         |
| 25      | 5000000 | 65        | 0.298       | 76923.077  | 0.253       | 0.376        | 0.917        | 1.376         | 50.331         |
| 30      | 5000000 | 65        | 0.288       | 76923.077  | 0.221       | 0.360        | 1.015        | 1.703         | 52.428         |
| 50      | 4999950 | 73        | 0.662       | 68492.466  | 0.294       | 0.622        | 5.767        | 23.068        | 65.011         |
| 100     | 5000000 | 79        | 1.361       | 63291.139  | 0.409       | 1.048        | 13.631       | 48.234        | 79.691         |
| 150     | 5000000 | 81        | 2.192       | 61728.395  | 0.442       | 1.245        | 24.117       | 52.428        | 75.497         |
| 200     | 5000000 | 94        | 3.540       | 53191.489  | 0.458       | 1.376        | 39.845       | 71.303        | 96.468         |

![lsm_get.png](perf_res%2Flsm%2Flsm_get.png)

### Remove
| threads | records | total sec | mean millis | ops/sec   | p0.5 millis | p0.75 millis | p0.99 millis | p0.999 millis | p0.9999 millis |
|---------|---------|-----------|-------------|-----------|-------------|--------------|--------------|---------------|----------------|
| 1       | 5000000 | 486       | 0.047       | 10288.066 | 0.038       | 0.040        | 0.130        | 0.179         | 0.228          |
| 5       | 5000000 | 137       | 0.061       | 36496.350 | 0.045       | 0.047        | 0.076        | 0.137         | 0.555          |
| 10      | 5000000 | 121       | 0.091       | 41322.314 | 0.068       | 0.080        | 0.170        | 0.293         | 1.898          |
| 15      | 5000000 | 121       | 0.136       | 41322.314 | 0.092       | 0.129        | 0.342        | 0.588         | 2.226          |
| 20      | 5000000 | 123       | 0.191       | 40650.407 | 0.129       | 0.186        | 0.653        | 1.112         | 3.275          |
| 25      | 5000000 | 125       | 0.245       | 40000.000 | 0.154       | 0.260        | 1.178        | 1.964         | 19.921         |
| 30      | 5000000 | 124       | 0.296       | 40322.581 | 0.154       | 0.309        | 1.702        | 2.882         | 24.115         |
| 50      | 5000000 | 128       | 0.511       | 39062.500 | 0.170       | 0.358        | 5.765        | 8.124         | 56.621         |
| 100     | 5000000 | 128       | 1.032       | 39062.500 | 0.162       | 0.326        | 16.775       | 22.018        | 88.078         |
| 150     | 5000000 | 129       | 1.571       | 38759.690 | 0.162       | 0.326        | 29.358       | 37.747        | 100.661        |
| 200     | 5000000 | 132       | 2.155       | 37878.788 | 0.170       | 0.326        | 41.941       | 52.427        | 121.633        |

![lsm_remove.png](perf_res%2Flsm%2Flsm_remove.png)

