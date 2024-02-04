package com.keks.kv_storage.lsm.conf;

import com.keks.kv_storage.conf.ParamParser;
import com.keks.kv_storage.ex.ConfigParameterParsingException;

import java.util.function.BiFunction;
import java.util.function.Function;

import static com.keks.kv_storage.conf.ConfigParams.*;


public enum LsmConfParamsEnum implements ParamParser {
    SYNC_WITH_THREAD_FLUSH(LSM_SYNC_WITH_THREAD_FLUSH, false, booleanParser),
    MAX_SSTABLES(LSM_MAX_SSTABLES, 50, 1, 16_384, intComparator, intParser),
    ENABLE_MERGE_IF_MAX_SSTABLES(LSM_ENABLE_MERGE_IF_MAX_SSTABLES, false, booleanParser),
    ENABLE_BACKGROUND_MERGE(LSM_ENABLE_BACKGROUND_MERGE, false, booleanParser),
    BACKGROUND_MERGE_INIT_DELAY(LSM_BACKGROUND_MERGE_INIT_DELAY, 60, 0, 108_000, intComparator, intParser),
    TRIGGER_BACKGROUND_MERGE_AFTER_SEC(LSM_TRIGGER_BACKGROUND_MERGE_AFTER_SEC, 3600, 1, 108_000, intComparator, intParser),

    SPARSE_INDEX_SIZE_RECORDS(LSM_SPARSE_INDEX_SIZE_RECORDS, 128, 1, 16_384, intComparator, intParser),
    MEM_CACHE_SIZE_RECORDS(LSM_MEM_CACHE_SIZE, 100000, 1, 10_000_000, intComparator, intParser),
    BLOOM_FILTER_FALSE_POSITIVE_RATE(LSM_BLOOM_FILTER_FALSE_POSITIVE_RATE, 0.5, 0.001, 0.99, doubleComparator, doubleParser);

    public final String name;
    public final Object defaultValue;

    private final Function<Object, Object> parser;
    private Object minValue;
    private Object maxValue;
    private BiFunction<Object, Object, Integer> comparator;

    LsmConfParamsEnum(String name, Object defaultValue, Function<Object, Object> parser) {
        this(name, defaultValue, null, null, null, parser);
    }

    LsmConfParamsEnum(String name,
                      Object defaultValue,
                      Object minValue,
                      Object maxValue,
                      BiFunction<Object, Object, Integer> comparator,
                      Function<Object, Object> parser) {
        this.name = name.toUpperCase();
        this.minValue = minValue;
        this.maxValue = maxValue;
        this.comparator = comparator;
        this.parser = parser;
        this.defaultValue = parse(defaultValue);
    }

    @Override
    public Object parse(Object obj) {
        Object value;
        try {
            value = parser.apply(obj);
        } catch (ClassCastException e) {
            throw new ConfigParameterParsingException(name, obj, e);
        }

        if (comparator != null) {
            if (comparator.apply(value, minValue) < 0)
                throw new IllegalArgumentException("Param '" + name + "' should be greater or equal '" + minValue + "'");
            if (comparator.apply(value, maxValue) > 0)
                throw new IllegalArgumentException("Param '" + name + "' should be lower or equal '" + maxValue + "'");
        }

        return value;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public Object getDefaultValue() {
        return defaultValue;
    }

}
