package com.keks.kv_storage.ex;

public class KVServerException extends KVStoreException {

    public KVServerException(String errorMessage) {
        super(errorMessage);
    }

    public KVServerException(String errorMessage, Throwable cause) {
        super(errorMessage, cause);
    }

}
