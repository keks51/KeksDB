package com.keks.kv_storage.lsm.io;

import com.keks.kv_storage.ex.KVStoreIOException;
import com.keks.kv_storage.io.JsonFile;
import com.keks.kv_storage.lsm.conf.LsmConf;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;


public class LsmConfJsonRW extends JsonFile {

    public final static String LSM_CONF_FILE_NAME = "lsm-conf.json";

    private final File jsonConfFile;

    public LsmConfJsonRW(Path dirPath) {
        this(dirPath.toFile());
    }

    public LsmConfJsonRW(File dirPath) {
        this.jsonConfFile = new File(dirPath, LSM_CONF_FILE_NAME);
    }

    public LsmConf read() {
        try {
            return new LsmConf(objectMapper.readTree(jsonConfFile));
        } catch (IOException e) {
            throw new KVStoreIOException("Cannot read " + LSM_CONF_FILE_NAME + " from path " + jsonConfFile.getAbsolutePath(), e);
        }
    }

    public void write(LsmConf lsmConf) {
        try {
            objectMapper.writeValue(jsonConfFile, lsmConf.getAsJson());
        } catch (IOException e) {
            throw new KVStoreIOException("Cannot write " + LSM_CONF_FILE_NAME + " to " + jsonConfFile.getAbsolutePath(), e);
        }
    }

}
