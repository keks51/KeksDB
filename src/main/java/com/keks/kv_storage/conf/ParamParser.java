package com.keks.kv_storage.conf;


import java.util.function.BiFunction;
import java.util.function.Function;

public interface ParamParser {
    String getName();
    Object getDefaultValue();

    Object parse(Object obj);

    Function<Object, Object> intParser = x -> Integer.valueOf(x.toString());
    Function<Object, Object> longParser = x -> Long.valueOf(x.toString());
    Function<Object, Object> booleanParser = x -> Boolean.valueOf(x.toString());
    Function<Object, Object> doubleParser = x -> Double.valueOf(x.toString());
    Function<Object, Object> strParser = Object::toString;

    BiFunction<Object, Object, Integer> intComparator = (x, y) -> Integer.compare((Integer) x, (Integer) y);
    BiFunction<Object, Object, Integer> longComparator = (x, y) -> Long.compare((Long) x,  (Long) y);

    BiFunction<Object, Object, Integer> doubleComparator = (x, y) -> Double.compare((Double) x, (Double) y);

}
