package com.keks.kv_storage.ex;

import java.io.File;


public class EngineDirNotFoundException extends KVStoreException {

    public EngineDirNotFoundException(File dir) {
        super("No engine dir exist in dir: " + dir);
    }

}
