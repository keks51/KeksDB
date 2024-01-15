package com.keks.kv_storage.ex;


public class EmptyKeyException extends KVStoreException {

    public EmptyKeyException() {
        super("Key could not be empty");
    }

}
