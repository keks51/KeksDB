package com.keks.kv_storage.lsm.query;

import com.keks.kv_storage.record.KVRecord;
import com.keks.kv_storage.query.range.MaxRangeKey;
import com.keks.kv_storage.query.range.MinRangeKey;
import com.keks.kv_storage.query.range.RangeSearchKey;
import org.junit.jupiter.api.Test;
import utils.TestUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;


class SsTableRangeIteratorTest {

    /**
     * 1 2 3 4 5 6 7 8
     * a) 2 <  x <  7
     * b) 2 <= x <= 7
     * c) 2 <  x <= 7
     * d) 2 <= x <  7
     */
    @Test
    public void test1() {

        ArrayList<KVRecord> data = new ArrayList<>() {{
            add(new KVRecord("1", "1"));
            add(new KVRecord("2", "2"));
            add(new KVRecord("3", "3"));
            add(new KVRecord("4", "4"));
            add(new KVRecord("5", "5"));
            add(new KVRecord("6", "6"));
            add(new KVRecord("7", "7"));
            add(new KVRecord("8", "8"));
        }};

        // a
        {
            AtomicInteger wasClosed = new AtomicInteger(0);

            try (SsTableRangeIterator rangeIterator = new SsTableRangeIterator(
                    data.iterator(),
                    new RangeSearchKey("2", false),
                    new RangeSearchKey("7", false),
                    wasClosed::incrementAndGet)
            ) {
                ArrayList<KVRecord> exp = new ArrayList<>() {{
                    add(new KVRecord("3", "3"));
                    add(new KVRecord("4", "4"));
                    add(new KVRecord("5", "5"));
                    add(new KVRecord("6", "6"));
                }};
                ArrayList<KVRecord> act = rangeIterator.getAsArr();
                assertEquals(exp, act);
            }
            assertEquals(1, wasClosed.get());
        }

        // b
        {
            AtomicInteger wasClosed = new AtomicInteger(0);

            try (SsTableRangeIterator rangeIterator = new SsTableRangeIterator(
                    data.iterator(),
                    new RangeSearchKey("2", true),
                    new RangeSearchKey("7", true),
                    wasClosed::incrementAndGet)
            ) {
                ArrayList<KVRecord> exp = new ArrayList<>() {{
                    add(new KVRecord("2", "2"));
                    add(new KVRecord("3", "3"));
                    add(new KVRecord("4", "4"));
                    add(new KVRecord("5", "5"));
                    add(new KVRecord("6", "6"));
                    add(new KVRecord("7", "7"));
                }};
                ArrayList<KVRecord> act = rangeIterator.getAsArr();
                assertEquals(exp, act);
            }
            assertEquals(1, wasClosed.get());
        }

        // c
        {
            AtomicInteger wasClosed = new AtomicInteger(0);

            try (SsTableRangeIterator rangeIterator = new SsTableRangeIterator(
                    data.iterator(),
                    new RangeSearchKey("2", false),
                    new RangeSearchKey("7", true),
                    wasClosed::incrementAndGet)
            ) {
                ArrayList<KVRecord> exp = new ArrayList<>() {{
                    add(new KVRecord("3", "3"));
                    add(new KVRecord("4", "4"));
                    add(new KVRecord("5", "5"));
                    add(new KVRecord("6", "6"));
                    add(new KVRecord("7", "7"));
                }};
                ArrayList<KVRecord> act = rangeIterator.getAsArr();
                assertEquals(exp, act);
            }
            assertEquals(1, wasClosed.get());
        }

        // d
        {
            AtomicInteger wasClosed = new AtomicInteger(0);

            try (SsTableRangeIterator rangeIterator = new SsTableRangeIterator(
                    data.iterator(),
                    new RangeSearchKey("2", true),
                    new RangeSearchKey("7", false),
                    wasClosed::incrementAndGet)
            ) {
                ArrayList<KVRecord> exp = new ArrayList<>() {{
                    add(new KVRecord("2", "2"));
                    add(new KVRecord("3", "3"));
                    add(new KVRecord("4", "4"));
                    add(new KVRecord("5", "5"));
                    add(new KVRecord("6", "6"));
                }};
                ArrayList<KVRecord> act = rangeIterator.getAsArr();
                assertEquals(exp, act);
            }
            assertEquals(1, wasClosed.get());
        }

    }

    /**
     * 3 4 5
     * a) 3 <  x <  5
     * b) 3 <= x <= 5
     * c) 3 <  x <= 5
     * d) 3 <= x < 5
     * -
     * e) 0 <= x < 3
     * f) 0 <= x <= 3
     * g) 0 <  x < 3
     * h) 0 <  x <= 3
     * -
     * i) 5 <  x <= 8
     * j) 5 <= x <= 8
     * k) 5 <  x <  8
     * l) 5 <= x <  8
     * -
     * m) 6 <  x <= 8
     * n) 6 <= x <= 8
     * o) 6 <  x <  8
     * p) 6 <= x <  8
     */
    @Test
    public void test2() {

        ArrayList<KVRecord> data = new ArrayList<>() {{
            add(new KVRecord("3", "3"));
            add(new KVRecord("4", "4"));
            add(new KVRecord("5", "5"));
        }};

        // a
        {
            AtomicInteger wasClosed = new AtomicInteger(0);

            try (SsTableRangeIterator rangeIterator = new SsTableRangeIterator(
                    data.iterator(),
                    new RangeSearchKey("3", false),
                    new RangeSearchKey("5", false),
                    wasClosed::incrementAndGet)
            ) {
                ArrayList<KVRecord> exp = new ArrayList<>() {{
                    add(new KVRecord("4", "4"));
                }};
                ArrayList<KVRecord> act = rangeIterator.getAsArr();
                assertEquals(exp, act);
            }
            assertEquals(1, wasClosed.get());
        }

        // b
        {
            AtomicInteger wasClosed = new AtomicInteger(0);

            try (SsTableRangeIterator rangeIterator = new SsTableRangeIterator(
                    data.iterator(),
                    new RangeSearchKey("3", true),
                    new RangeSearchKey("5", true),
                    wasClosed::incrementAndGet)
            ) {
                ArrayList<KVRecord> exp = new ArrayList<>() {{
                    add(new KVRecord("3", "3"));
                    add(new KVRecord("4", "4"));
                    add(new KVRecord("5", "5"));
                }};
                ArrayList<KVRecord> act = rangeIterator.getAsArr();
                assertEquals(exp, act);
            }
            assertEquals(1, wasClosed.get());
        }

        // c
        {
            AtomicInteger wasClosed = new AtomicInteger(0);

            try (SsTableRangeIterator rangeIterator = new SsTableRangeIterator(
                    data.iterator(),
                    new RangeSearchKey("3", false),
                    new RangeSearchKey("5", true),
                    wasClosed::incrementAndGet)
            ) {
                ArrayList<KVRecord> exp = new ArrayList<>() {{
                    add(new KVRecord("4", "4"));
                    add(new KVRecord("5", "5"));
                }};
                ArrayList<KVRecord> act = rangeIterator.getAsArr();
                assertEquals(exp, act);
            }
            assertEquals(1, wasClosed.get());
        }

        // d
        {
            AtomicInteger wasClosed = new AtomicInteger(0);

            try (SsTableRangeIterator rangeIterator = new SsTableRangeIterator(
                    data.iterator(),
                    new RangeSearchKey("3", true),
                    new RangeSearchKey("5", false),
                    wasClosed::incrementAndGet)
            ) {
                ArrayList<KVRecord> exp = new ArrayList<>() {{
                    add(new KVRecord("3", "3"));
                    add(new KVRecord("4", "4"));
                }};
                ArrayList<KVRecord> act = rangeIterator.getAsArr();
                assertEquals(exp, act);
            }
            assertEquals(1, wasClosed.get());
        }

        // e
        {
            AtomicInteger wasClosed = new AtomicInteger(0);

            try (SsTableRangeIterator rangeIterator = new SsTableRangeIterator(
                    data.iterator(),
                    new RangeSearchKey("0", true),
                    new RangeSearchKey("3", false),
                    wasClosed::incrementAndGet)
            ) {
                ArrayList<KVRecord> act = rangeIterator.getAsArr();
                assertTrue(act.isEmpty());
            }
            assertEquals(1, wasClosed.get());
        }

        // f
        {
            AtomicInteger wasClosed = new AtomicInteger(0);

            try (SsTableRangeIterator rangeIterator = new SsTableRangeIterator(
                    data.iterator(),
                    new RangeSearchKey("0", true),
                    new RangeSearchKey("3", true),
                    wasClosed::incrementAndGet)
            ) {
                ArrayList<KVRecord> exp = new ArrayList<>() {{
                    add(new KVRecord("3", "3"));
                }};
                ArrayList<KVRecord> act = rangeIterator.getAsArr();
                assertEquals(exp, act);
            }
            assertEquals(1, wasClosed.get());
        }

        // g
        {
            AtomicInteger wasClosed = new AtomicInteger(0);

            try (SsTableRangeIterator rangeIterator = new SsTableRangeIterator(
                    data.iterator(),
                    new RangeSearchKey("0", false),
                    new RangeSearchKey("3", false),
                    wasClosed::incrementAndGet)
            ) {
                ArrayList<KVRecord> act = rangeIterator.getAsArr();
                assertTrue(act.isEmpty());
            }
            assertEquals(1, wasClosed.get());
        }

        // h
        {
            AtomicInteger wasClosed = new AtomicInteger(0);

            try (SsTableRangeIterator rangeIterator = new SsTableRangeIterator(
                    data.iterator(),
                    new RangeSearchKey("0", false),
                    new RangeSearchKey("3", true),
                    wasClosed::incrementAndGet)
            ) {
                ArrayList<KVRecord> exp = new ArrayList<>() {{
                    add(new KVRecord("3", "3"));
                }};
                ArrayList<KVRecord> act = rangeIterator.getAsArr();
                assertEquals(exp, act);
            }
            assertEquals(1, wasClosed.get());
        }

        // i
        {
            AtomicInteger wasClosed = new AtomicInteger(0);

            try (SsTableRangeIterator rangeIterator = new SsTableRangeIterator(
                    data.iterator(),
                    new RangeSearchKey("5", false),
                    new RangeSearchKey("8", true),
                    wasClosed::incrementAndGet)
            ) {
                ArrayList<KVRecord> act = rangeIterator.getAsArr();
                assertTrue(act.isEmpty());
            }
            assertEquals(1, wasClosed.get());
        }

        // j
        {
            AtomicInteger wasClosed = new AtomicInteger(0);

            try (SsTableRangeIterator rangeIterator = new SsTableRangeIterator(
                    data.iterator(),
                    new RangeSearchKey("5", true),
                    new RangeSearchKey("8", true),
                    wasClosed::incrementAndGet)
            ) {
                ArrayList<KVRecord> exp = new ArrayList<>() {{
                    add(new KVRecord("5", "5"));
                }};
                ArrayList<KVRecord> act = rangeIterator.getAsArr();
                assertEquals(exp, act);
            }
            assertEquals(1, wasClosed.get());
        }

        // k
        {
            AtomicInteger wasClosed = new AtomicInteger(0);

            try (SsTableRangeIterator rangeIterator = new SsTableRangeIterator(
                    data.iterator(),
                    new RangeSearchKey("5", false),
                    new RangeSearchKey("8", false),
                    wasClosed::incrementAndGet)
            ) {
                ArrayList<KVRecord> act = rangeIterator.getAsArr();
                assertTrue(act.isEmpty());
            }
            assertEquals(1, wasClosed.get());
        }

        // k
        {
            AtomicInteger wasClosed = new AtomicInteger(0);

            try (SsTableRangeIterator rangeIterator = new SsTableRangeIterator(
                    data.iterator(),
                    new RangeSearchKey("5", true),
                    new RangeSearchKey("8", false),
                    wasClosed::incrementAndGet)
            ) {
                ArrayList<KVRecord> exp = new ArrayList<>() {{
                    add(new KVRecord("5", "5"));
                }};
                ArrayList<KVRecord> act = rangeIterator.getAsArr();
                assertEquals(exp, act);
            }
            assertEquals(1, wasClosed.get());
        }

        // m
        {
            AtomicInteger wasClosed = new AtomicInteger(0);

            try (SsTableRangeIterator rangeIterator = new SsTableRangeIterator(
                    data.iterator(),
                    new RangeSearchKey("6", false),
                    new RangeSearchKey("8", true),
                    wasClosed::incrementAndGet)
            ) {
                ArrayList<KVRecord> act = rangeIterator.getAsArr();
                assertTrue(act.isEmpty());
            }
            assertEquals(1, wasClosed.get());
        }

        // n
        {
            AtomicInteger wasClosed = new AtomicInteger(0);

            try (SsTableRangeIterator rangeIterator = new SsTableRangeIterator(
                    data.iterator(),
                    new RangeSearchKey("6", true),
                    new RangeSearchKey("8", true),
                    wasClosed::incrementAndGet)
            ) {
                ArrayList<KVRecord> act = rangeIterator.getAsArr();
                assertTrue(act.isEmpty());
            }
            assertEquals(1, wasClosed.get());
        }

        // o
        {
            AtomicInteger wasClosed = new AtomicInteger(0);

            try (SsTableRangeIterator rangeIterator = new SsTableRangeIterator(
                    data.iterator(),
                    new RangeSearchKey("6", false),
                    new RangeSearchKey("8", false),
                    wasClosed::incrementAndGet)
            ) {
                ArrayList<KVRecord> act = rangeIterator.getAsArr();
                assertTrue(act.isEmpty());
            }
            assertEquals(1, wasClosed.get());
        }

        // p
        {
            AtomicInteger wasClosed = new AtomicInteger(0);

            try (SsTableRangeIterator rangeIterator = new SsTableRangeIterator(
                    data.iterator(),
                    new RangeSearchKey("6", true),
                    new RangeSearchKey("8", false),
                    wasClosed::incrementAndGet)
            ) {
                ArrayList<KVRecord> act = rangeIterator.getAsArr();
                assertTrue(act.isEmpty());
            }
            assertEquals(1, wasClosed.get());
        }

    }

    /**
     * 3 4 5
     * a) min <  x <  5
     * b) min    x <= 5
     * c)   3 <  x
     * d)   3 <= x
     * e) min <  x < max
     * d)    5 < x <= 6
     * f)   2 <= x < 3
     */
    @Test
    public void test3() {

        ArrayList<KVRecord> data = new ArrayList<>() {{
            add(new KVRecord("3", "3"));
            add(new KVRecord("4", "4"));
            add(new KVRecord("5", "5"));
        }};

        // a
        {
            AtomicInteger wasClosed = new AtomicInteger(0);

            try (SsTableRangeIterator rangeIterator = new SsTableRangeIterator(
                    data.iterator(),
                    new MinRangeKey(),
                    new RangeSearchKey("5", false),
                    wasClosed::incrementAndGet)
            ) {
                ArrayList<KVRecord> exp = new ArrayList<>() {{
                    add(new KVRecord("3", "3"));
                    add(new KVRecord("4", "4"));
                }};
                ArrayList<KVRecord> act = rangeIterator.getAsArr();
                assertEquals(exp, act);
            }
            assertEquals(1, wasClosed.get());
        }

        // b
        {
            AtomicInteger wasClosed = new AtomicInteger(0);

            try (SsTableRangeIterator rangeIterator = new SsTableRangeIterator(
                    data.iterator(),
                    new MinRangeKey(),
                    new RangeSearchKey("5", true),
                    wasClosed::incrementAndGet)
            ) {
                ArrayList<KVRecord> exp = new ArrayList<>() {{
                    add(new KVRecord("3", "3"));
                    add(new KVRecord("4", "4"));
                    add(new KVRecord("5", "5"));
                }};
                ArrayList<KVRecord> act = rangeIterator.getAsArr();
                assertEquals(exp, act);
            }
            assertEquals(1, wasClosed.get());
        }

        // c
        {
            AtomicInteger wasClosed = new AtomicInteger(0);

            try (SsTableRangeIterator rangeIterator = new SsTableRangeIterator(
                    data.iterator(),
                    new RangeSearchKey("3", false),
                    new MaxRangeKey(),
                    wasClosed::incrementAndGet)
            ) {
                ArrayList<KVRecord> exp = new ArrayList<>() {{
                    add(new KVRecord("4", "4"));
                    add(new KVRecord("5", "5"));
                }};
                ArrayList<KVRecord> act = rangeIterator.getAsArr();
                assertEquals(exp, act);
            }
            assertEquals(1, wasClosed.get());
        }

        // d
        {
            AtomicInteger wasClosed = new AtomicInteger(0);

            try (SsTableRangeIterator rangeIterator = new SsTableRangeIterator(
                    data.iterator(),
                    new RangeSearchKey("3", true),
                    new MaxRangeKey(),
                    wasClosed::incrementAndGet)
            ) {
                ArrayList<KVRecord> exp = new ArrayList<>() {{
                    add(new KVRecord("3", "3"));
                    add(new KVRecord("4", "4"));
                    add(new KVRecord("5", "5"));
                }};
                ArrayList<KVRecord> act = rangeIterator.getAsArr();
                assertEquals(exp, act);
            }
            assertEquals(1, wasClosed.get());
        }

        // e
        {
            AtomicInteger wasClosed = new AtomicInteger(0);

            try (SsTableRangeIterator rangeIterator = new SsTableRangeIterator(
                    data.iterator(),
                    new MinRangeKey(),
                    new MaxRangeKey(),
                    wasClosed::incrementAndGet)
            ) {
                ArrayList<KVRecord> exp = new ArrayList<>() {{
                    add(new KVRecord("3", "3"));
                    add(new KVRecord("4", "4"));
                    add(new KVRecord("5", "5"));
                }};
                ArrayList<KVRecord> act = rangeIterator.getAsArr();
                assertEquals(exp, act);
            }
            assertEquals(1, wasClosed.get());
        }

        // d
        {
            AtomicInteger wasClosed = new AtomicInteger(0);

            try (SsTableRangeIterator rangeIterator = new SsTableRangeIterator(
                    data.iterator(),
                    new RangeSearchKey("5", false),
                    new RangeSearchKey("6", true),
                    wasClosed::incrementAndGet)
            ) {
                ArrayList<KVRecord> act = rangeIterator.getAsArr();
                assertTrue(act.isEmpty());
            }
            assertEquals(1, wasClosed.get());
        }

        // f
        {
            AtomicInteger wasClosed = new AtomicInteger(0);

            try (SsTableRangeIterator rangeIterator = new SsTableRangeIterator(
                    data.iterator(),
                    new RangeSearchKey("2", true),
                    new RangeSearchKey("3", false),
                    wasClosed::incrementAndGet)
            ) {
                ArrayList<KVRecord> act = rangeIterator.getAsArr();
                assertTrue(act.isEmpty());
            }
            assertEquals(1, wasClosed.get());
        }

    }


    // <= x <=
    @Test
    public void test4() {
        int minRecNum = 200;
        int maxRecNum = 800;
        ArrayList<KVRecord> allRecords = new ArrayList<>();
        ArrayList<KVRecord> recordsToSave = new ArrayList<>();

        {
            for (int i = 0; i < minRecNum; i++) {
                allRecords.add(null);
            }
            for (int i = minRecNum; i < maxRecNum; i++) {
                if (i % 5 == 0) {
                    KVRecord kvRecord = new KVRecord("key" + String.format("%07d", i), ("value" + i).getBytes());
                    recordsToSave.add(kvRecord);
                    allRecords.add(kvRecord);
                } else {
                    allRecords.add(null);
                }
            }
            for (int i = maxRecNum; i < maxRecNum + 200; i++) {
                allRecords.add(null);
            }
        }

        {

            for (int l = 0; l < maxRecNum + 200; l = l + 1) {
                String leftKey = "key" + String.format("%07d", l);
                for (int r = l + 50; r < maxRecNum + 200; r = r + 1) {
                    String rightKey = "key" + String.format("%07d", r);
                    List<KVRecord> kvRecords = allRecords.subList(l, r + 1);
                    List<KVRecord> exp = kvRecords.stream().filter(Objects::nonNull).collect(Collectors.toList());
                    AtomicInteger wasClosed = new AtomicInteger(0);

                    try (
                        SsTableRangeIterator rangeIterator = new SsTableRangeIterator(
                                recordsToSave.iterator(),
                                new RangeSearchKey(leftKey, true),
                                new RangeSearchKey(rightKey, true),
                                wasClosed::incrementAndGet)
                    ) {
                        ArrayList<KVRecord> asArr = rangeIterator.getAsArr();
                        TestUtils.iterateAndAssert(exp.iterator(), asArr.iterator());
                    }
                    assertEquals(1, wasClosed.get());


                }
            }

        }
    }

    // <= x <
    @Test
    public void test5() {
        int minRecNum = 200;
        int maxRecNum = 800;
        ArrayList<KVRecord> allRecords = new ArrayList<>();
        ArrayList<KVRecord> recordsToSave = new ArrayList<>();

        {
            for (int i = 0; i < minRecNum; i++) {
                allRecords.add(null);
            }
            for (int i = minRecNum; i < maxRecNum; i++) {
                if (i % 5 == 0) {
                    KVRecord kvRecord = new KVRecord("key" + String.format("%07d", i), ("value" + i).getBytes());
                    recordsToSave.add(kvRecord);
                    allRecords.add(kvRecord);
                } else {
                    allRecords.add(null);
                }
            }
            for (int i = maxRecNum; i < maxRecNum + 200; i++) {
                allRecords.add(null);
            }
        }

        {

            for (int l = 0; l < maxRecNum + 200; l = l + 1) {
                String leftKey = "key" + String.format("%07d", l);
                for (int r = l + 50; r < maxRecNum + 200; r = r + 1) {
                    String rightKey = "key" + String.format("%07d", r);
                    List<KVRecord> kvRecords = allRecords.subList(l, r + 1);
                    List<KVRecord> exp = kvRecords.stream().filter(Objects::nonNull).collect(Collectors.toList());
                    if (exp.size() > 0 && exp.get(exp.size() - 1).key.equals(rightKey)) { // right bound exclusive
                        exp.remove(exp.size() - 1);
                    }

                    AtomicInteger wasClosed = new AtomicInteger(0);

                    try (
                            SsTableRangeIterator rangeIterator = new SsTableRangeIterator(
                                    recordsToSave.iterator(),
                                    new RangeSearchKey(leftKey, true),
                                    new RangeSearchKey(rightKey, false),
                                    wasClosed::incrementAndGet)
                    ) {
                        ArrayList<KVRecord> asArr = rangeIterator.getAsArr();
                        TestUtils.iterateAndAssert(exp.iterator(), asArr.iterator());
                    }
                    assertEquals(1, wasClosed.get());


                }
            }

        }
    }

    // < x <=
    @Test
    public void test6() {
        int minRecNum = 200;
        int maxRecNum = 800;
        ArrayList<KVRecord> allRecords = new ArrayList<>();
        ArrayList<KVRecord> recordsToSave = new ArrayList<>();

        {
            for (int i = 0; i < minRecNum; i++) {
                allRecords.add(null);
            }
            for (int i = minRecNum; i < maxRecNum; i++) {
                if (i % 5 == 0) {
                    KVRecord kvRecord = new KVRecord("key" + String.format("%07d", i), ("value" + i).getBytes());
                    recordsToSave.add(kvRecord);
                    allRecords.add(kvRecord);
                } else {
                    allRecords.add(null);
                }
            }
            for (int i = maxRecNum; i < maxRecNum + 200; i++) {
                allRecords.add(null);
            }
        }

        {

            for (int l = 0; l < maxRecNum + 200; l = l + 1) {
                String leftKey = "key" + String.format("%07d", l);
                for (int r = l + 50; r < maxRecNum + 200; r = r + 1) {
                    String rightKey = "key" + String.format("%07d", r);
                    List<KVRecord> kvRecords = allRecords.subList(l, r + 1);
                    List<KVRecord> exp = kvRecords.stream().filter(Objects::nonNull).collect(Collectors.toList());
                    if (exp.size() > 0 && exp.get(0).key.equals(leftKey)) { // left bound exclusive
                        exp.remove(0);
                    }

                    AtomicInteger wasClosed = new AtomicInteger(0);

                    try (
                            SsTableRangeIterator rangeIterator = new SsTableRangeIterator(
                                    recordsToSave.iterator(),
                                    new RangeSearchKey(leftKey, false),
                                    new RangeSearchKey(rightKey, true),
                                    wasClosed::incrementAndGet)
                    ) {
                        ArrayList<KVRecord> asArr = rangeIterator.getAsArr();
                        TestUtils.iterateAndAssert(exp.iterator(), asArr.iterator());
                    }
                    assertEquals(1, wasClosed.get());


                }
            }

        }
    }

    // < x <
    @Test
    public void test7() {
        int minRecNum = 200;
        int maxRecNum = 800;
        ArrayList<KVRecord> allRecords = new ArrayList<>();
        ArrayList<KVRecord> recordsToSave = new ArrayList<>();

        {
            for (int i = 0; i < minRecNum; i++) {
                allRecords.add(null);
            }
            for (int i = minRecNum; i < maxRecNum; i++) {
                if (i % 5 == 0) {
                    KVRecord kvRecord = new KVRecord("key" + String.format("%07d", i), ("value" + i).getBytes());
                    recordsToSave.add(kvRecord);
                    allRecords.add(kvRecord);
                } else {
                    allRecords.add(null);
                }
            }
            for (int i = maxRecNum; i < maxRecNum + 200; i++) {
                allRecords.add(null);
            }
        }

        {

            for (int l = 0; l < maxRecNum + 200; l = l + 1) {
                String leftKey = "key" + String.format("%07d", l);
                for (int r = l + 50; r < maxRecNum + 200; r = r + 1) {
                    String rightKey = "key" + String.format("%07d", r);
                    List<KVRecord> kvRecords = allRecords.subList(l, r + 1);
                    List<KVRecord> exp = kvRecords.stream().filter(Objects::nonNull).collect(Collectors.toList());
                    if (exp.size() > 0 && exp.get(0).key.equals(leftKey)) { // left bound exclusive
                        exp.remove(0);
                    }

                    if (exp.size() > 0 && exp.get(exp.size() - 1).key.equals(rightKey)) { // right bound exclusive
                        exp.remove(exp.size() - 1);
                    }

                    AtomicInteger wasClosed = new AtomicInteger(0);

                    try (
                            SsTableRangeIterator rangeIterator = new SsTableRangeIterator(
                                    recordsToSave.iterator(),
                                    new RangeSearchKey(leftKey, false),
                                    new RangeSearchKey(rightKey, false),
                                    wasClosed::incrementAndGet)
                    ) {
                        ArrayList<KVRecord> asArr = rangeIterator.getAsArr();
                        TestUtils.iterateAndAssert(exp.iterator(), asArr.iterator());
                    }
                    assertEquals(1, wasClosed.get());


                }
            }

        }
    }

}