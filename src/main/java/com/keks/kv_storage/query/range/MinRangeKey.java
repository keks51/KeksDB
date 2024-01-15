package com.keks.kv_storage.query.range;


final public class MinRangeKey implements RangeKey {

    public MinRangeKey() {

    }

    @Override
    public boolean isGreater(String rightKey) {
        return false;
    }

    @Override
    public boolean isLower(String rightKey) {
        return true;
    }

    @Override
    public boolean isInclusive() {
        return true;
    }

    @Override
    public boolean isExclusive() {
        return false;
    }

    @Override
    public boolean isEqual(String rightKey) {
        return false;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        return o != null && getClass() == o.getClass();
    }

}
