package com.keks.kv_storage.bplus.conf;

import com.keks.kv_storage.conf.ParamParser;

import java.util.function.BiFunction;
import java.util.function.Function;

import static com.keks.kv_storage.conf.ConfigParams.*;


public enum BPlusConfParamsEnum implements ParamParser {
    BTREE_ORDER (BPLUS_TREE_ORDER, 400, 3, BtreeConf.MAX_ORDER, intComparator, intParser),
    BTREE_PAGE_BUFFER_SIZE_BYTES(BPLUS_TREE_PAGE_BUFFER_SIZE_BYTES, BtreeConf.DEFAULT_PAGE_BUFFER_SIZE_BYTES, 100_000L, Long.MAX_VALUE, longComparator, longParser),
    FREE_SPACE_CHECKER_CACHE_MAX(BPLUS_FREE_SPACE_CHECKER_CACHE_MAX, 10, 1, 1022, intComparator, intParser),
    FREE_SPACE_CHECKER_CACHE_INIT(BPLUS_FREE_SPACE_CHECKER_CACHE_INIT, 10, 1, 1022, intComparator, intParser);

    public final String name;
    public final Object defaultValue;

    private Object minValue;
    private Object maxValue;
    private BiFunction<Object, Object, Integer> comparator;
    private final Function<Object, Object> parser;

    BPlusConfParamsEnum(String name, Object defaultValue, Function<Object, Object> parser) {
        this(name, defaultValue, null, null, null, parser);
    }

    BPlusConfParamsEnum(String name,
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
        Object value = parser.apply(obj);
        if (comparator != null) {
            if (comparator.apply(value, minValue) < 0 )
                throw new IllegalArgumentException("Param '" + name + "' should be greater or equal '" + minValue + "'");
            if (comparator.apply(value, maxValue) > 0 )
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

