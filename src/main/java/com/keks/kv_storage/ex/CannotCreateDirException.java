package com.keks.kv_storage.ex;

import java.io.File;


public class CannotCreateDirException extends KVStoreException {

    public CannotCreateDirException(File dir) {
        super(dir.getAbsolutePath());
    }

}
