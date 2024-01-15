package com.keks.kv_storage.query;

import com.keks.kv_storage.query.range.MaxRangeKey;
import com.keks.kv_storage.query.range.MinRangeKey;
import com.keks.kv_storage.query.range.RangeSearchKey;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;


class RangeSearchKeyTest {

    @Test
    public void testRangeKey() {
        RangeSearchKey searchedKey = new RangeSearchKey("key05", false);

        assertFalse(searchedKey.isGreater("key05"));
        assertFalse(searchedKey.isLower("key05"));

        assertTrue(searchedKey.isGreater("key04"));
        assertFalse(searchedKey.isLower("key04"));

        assertFalse(searchedKey.isGreater("key06"));
        assertTrue(searchedKey.isLower("key06"));
    }

    @Test
    public void testMinKey() {
        assertFalse(new MinRangeKey().isGreater("key06"));
        assertTrue(new MinRangeKey().isLower("key06"));
    }

    @Test
    public void testMaxKey() {
        assertTrue(new MaxRangeKey().isGreater("key06"));
        assertFalse(new MaxRangeKey().isLower("key06"));
    }

}