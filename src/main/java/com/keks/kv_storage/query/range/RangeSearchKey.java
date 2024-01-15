package com.keks.kv_storage.query.range;


import java.util.Objects;

final public class RangeSearchKey implements RangeKey {

    public final String key;
    private final boolean inclusive;

    public RangeSearchKey(String key, boolean inclusive) {
        this.key = key;
        this.inclusive = inclusive;
    }

    @Override
    public boolean isGreater(String rightKey) {
        return key.compareTo(rightKey) > 0;
    }

    @Override
    public boolean isLower(String rightKey) {
        return key.compareTo(rightKey) < 0;
    }

    @Override
    public boolean isInclusive() {
        return inclusive;
    }

    @Override
    public boolean isExclusive() {
        return !inclusive;
    }

    @Override
    public boolean isEqual(String rightKey) {
        return key.equals(rightKey);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RangeSearchKey searchKey = (RangeSearchKey) o;
        return inclusive == searchKey.inclusive && Objects.equals(key, searchKey.key);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, inclusive);
    }
}
