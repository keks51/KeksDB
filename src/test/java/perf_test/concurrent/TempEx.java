package perf_test.concurrent;

import com.keks.kv_storage.record.KVRecord;
import com.keks.kv_storage.lsm.LsmEngine;
import com.keks.kv_storage.lsm.conf.LsmConf;
import com.keks.kv_storage.query.Query;
import com.keks.kv_storage.query.QueryIterator;
import com.keks.kv_storage.utils.SimpleScheduler;
import com.keks.kv_storage.utils.Time;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.sql.Timestamp;
import java.time.*;

class TempExTest {



    SimpleScheduler scheduler = new SimpleScheduler();

    int SECONDS_IN_DAY = 60 * 60 * 24; // 86_400

    @Test
    public void test124321(@TempDir Path tmpPath) throws IOException {
        String kvTableName = "temp_table";
        LsmConf lsmConf = new LsmConf(400, 100_000, 0.5, false, 1000);
        File tableDir = tmpPath.resolve(kvTableName).toFile();
        tableDir.mkdir();
        LsmEngine lsmEngine = LsmEngine.createNewTable("1", tableDir, lsmConf, scheduler);


        long SEC_OFFSET = 1_691_323_614;

//        LocalDateTime dateTime = LocalDateTime.parse("2023-08-06T11:50:55");
//
//        ZonedDateTime zdt = dateTime.atZone(ZoneId.of("UTC"));
//
//
////        Timestamp timestamp = new Timestamp(SEC_OFFSET * 1000);
//        System.out.println(zdt.toInstant().getEpochSecond());
//        Timestamp timestamp = new Timestamp(zdt.toInstant().toEpochMilli());
//
//
//        System.out.println(timestamp);
//        System.out.println(Integer.MIN_VALUE);
//
        int sensorsCnt = 10;
        int secondsCnt = SECONDS_IN_DAY * 30; // 2_592_000
//        int secondsCnt = 60; // 2_592_000
//
//

        for (int sec = 0; sec < secondsCnt; sec++) {
            for (int sensId = 0; sensId < sensorsCnt; sensId++) {
                long curSec = SEC_OFFSET + sec;
                String key = sensId + "_" + curSec;
                String value = "24.56";
                lsmEngine.put(new KVRecord(key, value));
                if (sec == 0 || sec == secondsCnt - 1) {
                    System.out.println(key + "   " + value);
                }
            }
        }

        lsmEngine.forceFlush();

        long minTime = LocalDateTime.parse("2023-08-07T11:00:00").toInstant(ZoneOffset.UTC).getEpochSecond();
        long maxTime = LocalDateTime.parse("2023-08-08T12:30:55").toInstant(ZoneOffset.UTC).getEpochSecond();

        // 0_1691323614   24.56
        // 0_1691755613   24.56
        int senId = 0;
        String minKey = senId + "_" + minTime;
        String maxKey = senId + "_" + maxTime;
        Query query = new Query.QueryBuilder().withMinKey(minKey, true).withMaxKey(maxKey, true).withNoLimit().build();

        // select : PT0.139584S
        Time.withTime("select", () -> {

            QueryIterator rangeRecords = lsmEngine.getRangeRecords(query);
            int cnt = 0;
            while (rangeRecords.hasNext()) {
                KVRecord next = rangeRecords.next();
                String key = next.key;
                String value = new String(next.valueBytes);
                String[] split = key.split("_");
                int sensId = Integer.parseInt(split[0]);
                long time = Long.parseLong(split[1]);
                LocalDateTime localDateTime = Instant.ofEpochSecond(time).atZone(ZoneOffset.UTC).toLocalDateTime();
                if (cnt % 100 == 0) {
                    System.out.println("SensorId: " + sensId + " Time: " + localDateTime + " Temp: " + value);
                }
                cnt++;
            }
        });


    }


    @Test
    public void test14134() {
//        long SEC_OFFSET = 1_691_323_614;
        long minTime = LocalDateTime.parse("2023-08-07T11:00:00").toInstant(ZoneOffset.UTC).toEpochMilli();

        System.out.println(LocalDateTime.parse("2023-08-07T11:00:00"));
        System.out.println(LocalDateTime.parse("2023-08-07T11:00:00").toInstant(ZoneOffset.UTC));
        System.out.println(new Timestamp(minTime));


    }

}
