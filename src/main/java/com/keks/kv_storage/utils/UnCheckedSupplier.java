package com.keks.kv_storage.utils;


@FunctionalInterface
public interface UnCheckedSupplier<T, EX extends Exception> {
    T get() throws EX;
}
