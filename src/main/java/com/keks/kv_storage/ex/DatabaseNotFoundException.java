package com.keks.kv_storage.ex;


public class DatabaseNotFoundException extends KVStoreException {

    public DatabaseNotFoundException(String databaseName) {
        super("Database: " + databaseName + " doesn't exist.");
    }

    public DatabaseNotFoundException(String databaseName, Throwable cause) {
        super("Database: " + databaseName + " doesn't exist.", cause);
    }


}
