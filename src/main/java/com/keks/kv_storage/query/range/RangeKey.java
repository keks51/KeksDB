package com.keks.kv_storage.query.range;


public interface RangeKey {

    boolean isGreater(String rightKey);

    boolean isLower(String rightKey);

    boolean isInclusive();

    boolean isExclusive();

    boolean isEqual(String rightKey);

}
