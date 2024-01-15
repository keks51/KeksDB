package com.keks.kv_storage.ex;

import java.util.ArrayList;


public class DatabaseNotEmptyException extends KVStoreException {

    public DatabaseNotEmptyException(String databaseName, ArrayList<String> tables) {
        super("Database: " + databaseName + " is not empty and contains tables: " + String.join(", ", tables));
    }

    public DatabaseNotEmptyException(String databaseName, ArrayList<String> tables, Throwable cause) {
        super("Database: " + databaseName + " is not empty and contains tables: " + String.join(", ", tables), cause);
    }


}
