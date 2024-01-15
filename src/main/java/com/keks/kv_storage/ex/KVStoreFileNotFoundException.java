package com.keks.kv_storage.ex;

import java.io.File;


public class KVStoreFileNotFoundException extends KVStoreException {

    public KVStoreFileNotFoundException(File file, Throwable cause) {
        super(file.getAbsolutePath() + " (No such file or directory)", cause);
    }

}
