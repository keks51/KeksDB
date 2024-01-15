package com.keks.kv_storage.ex;


public class KVStoreException extends RuntimeException {

    public KVStoreException(String errorMessage) {
        super(errorMessage);
    }

    public KVStoreException(String errorMessage, Throwable cause) {
        super(errorMessage, cause);
    }

}
