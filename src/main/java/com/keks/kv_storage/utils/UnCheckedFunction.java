package com.keks.kv_storage.utils;


@FunctionalInterface
public interface UnCheckedFunction<T, R, EX extends Exception> {

    R apply(T t) throws EX;

}
