package com.keks.kv_storage.query;

import com.fasterxml.jackson.databind.JsonNode;
import com.keks.kv_storage.query.range.MaxRangeKey;
import com.keks.kv_storage.query.range.MinRangeKey;
import com.keks.kv_storage.query.range.RangeSearchKey;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;


class QueryTest {

    @Test
    public void test1() {
        Query query = new Query(new RangeSearchKey("10", false), new MaxRangeKey(), Integer.MAX_VALUE);
        JsonNode json = query.toJson();
        System.out.println(json.toPrettyString());

        Query parsed = Query.fromJson(json);

        assertEquals(query, parsed);
    }

    @Test
    public void test2() {
        Query query = new Query(new RangeSearchKey("10", true), new MaxRangeKey(), Integer.MAX_VALUE);
        JsonNode json = query.toJson();
        System.out.println(json.toPrettyString());

        Query parsed = Query.fromJson(json);
        assertEquals(query, parsed);
    }

    @Test
    public void test3() {
        Query query = new Query(new MinRangeKey(), new RangeSearchKey("10", false), Integer.MAX_VALUE);
        JsonNode json = query.toJson();
        System.out.println(json.toPrettyString());

        Query parsed = Query.fromJson(json);
        assertEquals(query, parsed);
    }

    @Test
    public void test4() {
        Query query = new Query(new MinRangeKey(), new RangeSearchKey("10", true), Integer.MAX_VALUE);
        JsonNode json = query.toJson();
        System.out.println(json.toPrettyString());

        Query parsed = Query.fromJson(json);
        assertEquals(query, parsed);
    }

    @Test
    public void test6() {
        Query query = new Query(new MinRangeKey(), new MaxRangeKey(), Integer.MAX_VALUE);
        JsonNode json = query.toJson();
        System.out.println(json.toPrettyString());

        Query parsed = Query.fromJson(json);
        assertEquals(query, parsed);
    }

    @Test
    public void test7() {
        Query query = new Query(new RangeSearchKey("10", false), new RangeSearchKey("11", true), Integer.MAX_VALUE);
        JsonNode json = query.toJson();
        System.out.println(json.toPrettyString());

        Query parsed = Query.fromJson(json);
        assertEquals(query, parsed);
    }

}