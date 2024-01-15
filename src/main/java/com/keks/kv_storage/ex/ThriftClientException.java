package com.keks.kv_storage.ex;


public class ThriftClientException extends KVStoreException {

    public ThriftClientException(String errorMessage) {
        super(errorMessage);
    }

}
