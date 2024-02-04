package perf_test;

import com.keks.kv_storage.record.KVRecord;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;


public class DataGenerator {

    public static void main(String[] args) {

        int recordsCnt = 7;
        int numOfBatches = 5;
//        int batchSize = recordsCnt / numOfBatches;
////        System.out.println(recordsCnt / batchSize);
////
//      IntStream.range(0, numOfBatches)
//                .mapToObj(batchNum -> {
//                            int start = batchNum * batchSize;
//                            int end = batchNum == numOfBatches - 1 ? recordsCnt : (batchNum + 1) * batchSize;
//                            return new int[]{start, end};
//                        }
//                )
//                .forEach(batch -> {
//                    System.out.println(batch[0] + ":" + batch[1]);
//                });
//////        IntStream.range(0, (records + batchSize - 1) / batchSize)
//////                .mapToObj(i -> new int[] {i * batchSize, Math.min(records, (i + 1) * batchSize) - 1})
//////                .forEach(batch -> {
//////                    System.out.println(batch[0] + ":" + batch[1]);
//////                });

        DataGenerator dataGenerator = new DataGenerator(recordsCnt, numOfBatches);
        for (Iterator<KVRecord> batch : dataGenerator.batches) {
            while (batch.hasNext()){
                System.out.println(batch.next());
            }
            System.out.println();
        }
    }

    private final ArrayList<Iterator<KVRecord>> batches;
    private AtomicLong totalBytes = new AtomicLong();

    private final ArrayList<Integer> list;

    public DataGenerator(int recordsCnt, int numOfBatches) {
        int batchSize = recordsCnt / numOfBatches;
        list = IntStream.range(0, recordsCnt).boxed().collect(Collectors.toCollection(ArrayList::new));
        Collections.shuffle(list);

        batches =  IntStream.range(0, numOfBatches)
                .mapToObj(batchNum -> {
                            int start = batchNum * batchSize;
                            int end = batchNum == numOfBatches - 1 ? recordsCnt : (batchNum + 1) * batchSize;
                            return new int[]{start, end};
                        }
                )
                .map(batch -> {
                    final int l = batch[0];
                    final int r = batch[1];
                    return new Iterator<KVRecord>() {
                        int start = l;
                        final int end = r;

                        @Override
                        public boolean hasNext() {
                            return start < end;
                        }

                        @Override
                        public KVRecord next() {
                            int x = list.get(start);
                            String key = "key" + String.format("%09d", x);
//                            String value = (("value" + String.format("%10d", x)).repeat(33)) + "abcd";
                            String value = "abcdefghijklmnopqrstuvwxyz".repeat(19) + "abcdef";
                            KVRecord kvRecord = new KVRecord(key, value);
                            totalBytes.addAndGet(key.length() + value.length());
                            start++;
                            return kvRecord;
                        }
                    };
                }).collect(Collectors.toCollection(ArrayList::new));

    }

    public Iterator<KVRecord> getBatchIter(int batchId) {
        return batches.get(batchId);
//        return null;
    }

    public Iterator<String> getBatchIterKeys(int batchId) {
        Iterator<KVRecord> iterator = batches.get(batchId);
        return new Iterator<String>() {
            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public String next() {
                return iterator.next().key;
            }
        };
    }

    public long getTotalBytes() {
        return totalBytes.get();
    }

}
