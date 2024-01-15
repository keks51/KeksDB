package com.keks.kv_storage.io;

import com.keks.kv_storage.ex.KVStoreFileNotFoundException;
import com.keks.kv_storage.ex.KVStoreIOException;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;


public abstract class RafFile implements java.lang.AutoCloseable {

    public final RandomAccessFile raf;
    public final File file;

    public RafFile(File file, String accessMode) {
        try {
            this.file = file;
            this.raf = new RandomAccessFile(file, accessMode);
        } catch (FileNotFoundException e) {
            throw new KVStoreFileNotFoundException(file, e);
        }
    }

    @Override
    public void close() {
        try {
            raf.close();
        } catch (IOException e) {
            throw new KVStoreIOException("Cannot close " + file.getAbsolutePath(), e);
        }
    }

}
