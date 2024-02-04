package com.keks.kv_storage.kv_table_conf;

import com.keks.kv_storage.conf.ParamParser;
import com.keks.kv_storage.ex.ConfigParameterParsingException;

import java.util.function.BiFunction;
import java.util.function.Function;

import static com.keks.kv_storage.conf.ConfigParams.*;


public enum KvTableConfParamsEnum implements ParamParser {

    ENABLE_WAL(KV_TABLE_ENABLE_WAL, true, booleanParser),
    COMMIT_LOG_PARALLELISM(KV_TABLE_COMMIT_LOG_PARALLELISM, 10, 1, 1000, intComparator, intParser),
    ENABLE_PERIODIC_CHECKPOINT(KV_TABLE_ENABLE_PERIODIC_CHECKPOINT, false, booleanParser),
    TRIGGER_CHECKPOINT_AFTER_SEC(KV_TABLE_TRIGGER_CHECKPOINT_AFTER_SEC, 10, 1, 108_000, intComparator, intParser);

    public final String name;
    public final Object defaultValue;

    private final Function<Object, Object> parser;
    private Object minValue;
    private Object maxValue;
    private BiFunction<Object, Object, Integer> comparator;

    KvTableConfParamsEnum(String name, Object defaultValue, Function<Object, Object> parser) {
        this(name, defaultValue, null, null, null, parser);
    }

    KvTableConfParamsEnum(String name,
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
