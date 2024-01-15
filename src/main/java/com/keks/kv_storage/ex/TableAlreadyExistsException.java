package com.keks.kv_storage.ex;


public class TableAlreadyExistsException extends KVStoreException {

    public TableAlreadyExistsException(String dbName, String tableName) {
        super("Table: " + dbName + "." + tableName + " already exists.");
    }

    public TableAlreadyExistsException(String dbName, String tableName, Throwable cause) {
        super("Table: " + dbName + "." + tableName + " already exists.", cause);
    }


}
