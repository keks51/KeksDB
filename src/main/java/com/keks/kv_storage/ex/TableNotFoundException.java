package com.keks.kv_storage.ex;


public class TableNotFoundException extends KVStoreException {

    public TableNotFoundException(String dbName, String tableName) {
        super("Table: " + dbName + "." + tableName + " doesn't exist.");
    }

    public TableNotFoundException(String dbName, String tableName, Throwable cause) {
        super("Table: " + dbName + "." + tableName + " doesn't exist.", cause);
    }


}
