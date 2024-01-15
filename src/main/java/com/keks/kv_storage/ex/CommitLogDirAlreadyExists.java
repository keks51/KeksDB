package com.keks.kv_storage.ex;

import java.io.File;


public class CommitLogDirAlreadyExists extends KVStoreException {

    public CommitLogDirAlreadyExists(File dir) {
        super("Commit log dir already exists: " + dir);
    }

}
