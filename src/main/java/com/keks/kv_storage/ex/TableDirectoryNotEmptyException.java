package com.keks.kv_storage.ex;

import java.io.File;


public class TableDirectoryNotEmptyException extends KVStoreException {

    public TableDirectoryNotEmptyException(String databaseName, String tableName, File path) {
        super("Table: " + databaseName + "." + tableName +" Cannot delete Table dir: '" + path + "'. Dir not empty.");
    }
}
