package com.keks.kv_storage.ex;


public class ConfigParameterParsingException extends KVStoreException {

    public ConfigParameterParsingException(String parameterName, Object value, Throwable cause) {
        super("Cannot parse: " + parameterName + " Value: '" + value + "'", cause);
    }

}
