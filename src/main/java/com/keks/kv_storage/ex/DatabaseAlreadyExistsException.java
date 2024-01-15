package com.keks.kv_storage.ex;


public class DatabaseAlreadyExistsException extends KVStoreException {

    public DatabaseAlreadyExistsException(String databaseName) {
        super("Database: " + databaseName + " already exists.");
    }

    public DatabaseAlreadyExistsException(String databaseName, Throwable cause) {
        super("Database: " + databaseName + " already exists.", cause);
    }


}
