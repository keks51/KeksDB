package com.keks.kv_storage.ex;


public class EmptyValueException extends KVStoreException {

    public EmptyValueException() {
        super("Value could not be empty");
    }

}
