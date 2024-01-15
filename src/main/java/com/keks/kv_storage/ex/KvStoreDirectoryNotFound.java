package com.keks.kv_storage.ex;

import java.io.File;


public class KvStoreDirectoryNotFound extends KVStoreException {

    public KvStoreDirectoryNotFound(File dir) {
        super("KvStore dir doesn't exist: " + dir);
    }

}
