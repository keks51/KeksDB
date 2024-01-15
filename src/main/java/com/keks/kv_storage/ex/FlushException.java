package com.keks.kv_storage.ex;


public class FlushException extends KVStoreException {

    public FlushException() {
        super("Cannot flush");
    }

    public FlushException(Throwable cause) {
        super("Cannot flush", cause);
    }


}
