package com.keks.kv_storage.ex;

public class ThriftClientCommandException extends ThriftClientException {

    public final String className;


    public ThriftClientCommandException(String className, String errorMessage) {
        super(errorMessage);
        this.className = className;
    }

}
