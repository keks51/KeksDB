package com.keks.kv_storage.utils;


@FunctionalInterface
public interface UnCheckedConsumer<T, EX extends Exception> {
    void accept(T e) throws EX;
}
