package com.keks.kv_storage.utils;

import java.util.Iterator;


public class IteratorUtils {

    public static <T> Iterator<T> concatIterator(Iterator<T> it1, Iterator<T> it2) {
        return new Iterator<>() {
            public boolean hasNext() {
                return it1.hasNext() || it2.hasNext();
            }
            public T next() {
                return it1.hasNext() ? it1.next() : it2.next();
            }
        };
    }

}
