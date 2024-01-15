package com.keks.kv_storage.utils;


@FunctionalInterface
public interface UnCheckedRunnable<EX extends Exception> {

    void run() throws EX;

}
