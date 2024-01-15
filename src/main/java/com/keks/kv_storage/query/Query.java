package com.keks.kv_storage.query;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.keks.kv_storage.query.range.MaxRangeKey;
import com.keks.kv_storage.query.range.MinRangeKey;
import com.keks.kv_storage.query.range.RangeKey;
import com.keks.kv_storage.query.range.RangeSearchKey;

import java.util.Objects;


public class Query {

    public static final Query QUERY_ALL = new QueryBuilder().build();

    public static final String MIN_VALUE_PROP = "MIN_VALUE";
    public static final String IS_MIN_INCLUSIVE_PROP = "IS_MIN_INCLUSIVE";
    public static final String MAX_VALUE_PROP = "MAX_VALUE";
    public static final String IS_MAX_INCLUSIVE_PROP = "IS_MAX_INCLUSIVE";

    public static final String LIMIT_PROP = "LIMIT";

    public final RangeKey min;
    public final RangeKey max ;
    public final long limit;

    public Query(RangeKey min, RangeKey max, long limit) {
        this.min = min;
        this.max = max;
        this.limit = limit;
    }

    public static class QueryBuilder {

        private RangeKey min;
        private RangeKey max;

        private long limit;

        public QueryBuilder() {
            this.min = new MinRangeKey();
            this.max = new MaxRangeKey();
            this.limit = Long.MAX_VALUE;
        }

        public QueryBuilder withNoMinBound() {
            this.min = new MinRangeKey();
            return this;
        }

        public QueryBuilder withMinKey(RangeKey key) {
            this.min = key;
            return this;
        }

        public QueryBuilder withMinKey(String key, boolean inclusive) {
            this.min = new RangeSearchKey(key, inclusive);
            return this;
        }

        public QueryBuilder withMaxKey(RangeKey max) {
            this.max = max;
            return this;
        }

        public QueryBuilder withNoMaxBound() {
            this.max = new MaxRangeKey();
            return this;
        }

        public QueryBuilder withMaxKey(String key, boolean inclusive) {
            this.max = new RangeSearchKey(key, inclusive);
            return this;
        }

        public QueryBuilder withNoLimit() {
            this.limit = Long.MAX_VALUE;
            return this;
        }

        public QueryBuilder withLimit(long limit) {
            assert limit > 0;
            this.limit = limit;
            return this;
        }

        public Query build() {
            return new Query(min, max, limit);
        }

    }

    public JsonNode toJson() {
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode rootNode = mapper.createObjectNode();
        if (min instanceof RangeSearchKey) {
            RangeSearchKey minKey = (RangeSearchKey) min;
            rootNode.put(MIN_VALUE_PROP, minKey.key);
            rootNode.put(IS_MIN_INCLUSIVE_PROP, minKey.isInclusive());
        }
        if (max instanceof RangeSearchKey) {
            RangeSearchKey maxKey = (RangeSearchKey) max;
            rootNode.put(MAX_VALUE_PROP, maxKey.key);
            rootNode.put(IS_MAX_INCLUSIVE_PROP, maxKey.isInclusive());
        }

        rootNode.put(LIMIT_PROP, limit);

        return rootNode;
    }

    public static Query fromJson(JsonNode jsonNode) {
        QueryBuilder builder = new Query.QueryBuilder();
        if (jsonNode.has(MIN_VALUE_PROP)) {
            builder = builder.withMinKey(
                    jsonNode.get(MIN_VALUE_PROP).textValue(),
                    jsonNode.get(IS_MIN_INCLUSIVE_PROP).booleanValue());
        }
        if (jsonNode.has(MAX_VALUE_PROP)) {
            builder = builder.withMaxKey(
                    jsonNode.get(MAX_VALUE_PROP).textValue(),
                    jsonNode.get(IS_MAX_INCLUSIVE_PROP).booleanValue());
        }

        builder = builder.withLimit(jsonNode.get(LIMIT_PROP).asLong());

        return builder.build();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Query query = (Query) o;
        boolean equals1 = min.equals(query.min);
        boolean equals2 = max.equals(query.max);
        return min.equals(query.min) && max.equals(query.max);
    }

    @Override
    public int hashCode() {
        return Objects.hash(min, max);
    }
}
