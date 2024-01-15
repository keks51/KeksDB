package com.keks.kv_storage.utils;


@FunctionalInterface
public interface UnCheckedBiConsumer<T, R, EX extends Exception> {

    void accept(T e, R r) throws EX;

}
