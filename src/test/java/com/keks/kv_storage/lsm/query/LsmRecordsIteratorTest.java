package com.keks.kv_storage.lsm.query;

import com.keks.kv_storage.record.KVRecord;
import com.keks.kv_storage.query.range.MaxRangeKey;
import com.keks.kv_storage.query.range.MinRangeKey;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;


class LsmRecordsIteratorTest {

    @Test
    public void test1() {
        //0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
        //a,b,c,d,e,f,g,h,i,j,k ,l ,m ,n, o, p
        List<KVRecord> strings1 = new ArrayList<>(Arrays.asList(
                new KVRecord("a", "1"),
                new KVRecord("b", "1"),
                new KVRecord("c", "1"),
                new KVRecord("d", "1"),
                new KVRecord("e"),
                new KVRecord("f", "1"),
                new KVRecord("g", "1"),
                new KVRecord("h", "1"),
                new KVRecord("j", "1"),
                new KVRecord("k"),
                new KVRecord("l", "1"),
                new KVRecord("m", "1"),
                new KVRecord("n")
        ));

        List<KVRecord> strings2 = new ArrayList<>(Arrays.asList(
                new KVRecord("b", "2"),
                new KVRecord("c"),
                new KVRecord("f"),
                new KVRecord("g", "2"),
                new KVRecord("i", "2"),
                new KVRecord("j"),
                new KVRecord("k"),
                new KVRecord("l"),
                new KVRecord("m"),
                new KVRecord("n", "2")
        ));

        List<KVRecord> strings3 = new ArrayList<>(Arrays.asList(
                new KVRecord("a", "3"),
                new KVRecord("b"),
                new KVRecord("c", "3"),
                new KVRecord("d"),
                new KVRecord("e"),
                new KVRecord("f", "3"),
                new KVRecord("g", "3"),
                new KVRecord("j", "3"),
                new KVRecord("k"),
                new KVRecord("l"),
                new KVRecord("m", "3"),
                new KVRecord("n", "3"),
                new KVRecord("o", "3"),
                new KVRecord("p")
        ));

        List<KVRecord> strings4 = new ArrayList<>(Arrays.asList(
                new KVRecord("a", "4")
        ));

        checkCompaction(strings4, strings3, strings2, strings1);
    }

    @Test
    public void test2() {
        List<KVRecord> strings1 = new ArrayList<>(Arrays.asList(
                new KVRecord("a", "1")
                ));
        List<KVRecord> strings2 = new ArrayList<>(Arrays.asList(
                new KVRecord("a", "1")
        ));

        checkCompaction(strings2, strings1);
    }

    @Test
    public void test3() {
        List<KVRecord> strings1 = new ArrayList<>(Arrays.asList(
                new KVRecord("a")
        ));
        List<KVRecord> strings2 = new ArrayList<>(Arrays.asList(
                new KVRecord("a", "1")
        ));

        checkCompaction(strings2, strings1);
    }

    @Test
    public void test4() {
        List<KVRecord> strings1 = new ArrayList<>(Arrays.asList(
                new KVRecord("a", "1")
        ));
        List<KVRecord> strings2 = new ArrayList<>(Arrays.asList(
                new KVRecord("a")
        ));

        checkCompaction(strings2, strings1);
    }

    @SafeVarargs
    private void checkCompaction(List<KVRecord>... recordLists) {
        ArrayList<KVRecord> res = buildRes(recordLists);
        ArrayList<KVRecord> exp = buildExp(recordLists);
        assertEquals(exp, res);
    }

    @SafeVarargs
    private ArrayList<KVRecord> buildRes(List<KVRecord>... recordLists) {
        List<SsTableRangeIterator> collect = Arrays
                .stream(recordLists).map(List::iterator)
                .map(iter -> new SsTableRangeIterator(
                        iter,
                        new MinRangeKey(),
                        new MaxRangeKey(), () ->{})
                ).collect(Collectors.toList());
        LsmRecordsIterator recordsMergerIterator = new LsmRecordsIterator(collect);
        ArrayList<KVRecord> res = new ArrayList<>();
        recordsMergerIterator.forEachRemaining(res::add);
        return res;
    }

    @SafeVarargs
    private ArrayList<KVRecord> buildExp(List<KVRecord>... recordLists1) {
        ArrayList<List<KVRecord>> recordsArray = new ArrayList<>(Arrays.asList(recordLists1));
        Collections.reverse(recordsArray);
        TreeMap<String, String> all = new TreeMap<>(String::compareTo);
        recordsArray.forEach(list -> list.forEach(e -> all.put(e.key, new String(e.valueBytes))));
        ArrayList<KVRecord> expectedRecords = new ArrayList<>();
        all.entrySet()
                .stream()
                .filter(e -> !e.getValue().equals(""))
                .collect(Collectors.toList())
                .forEach(e -> expectedRecords.add(new KVRecord(e.getKey() ,e.getValue())));
        return expectedRecords;
    }

}
