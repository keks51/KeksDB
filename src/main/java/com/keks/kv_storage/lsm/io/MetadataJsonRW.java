package com.keks.kv_storage.lsm.io;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.keks.kv_storage.io.JsonFile;
import com.keks.kv_storage.ex.KVStoreIOException;
import com.keks.kv_storage.lsm.ss_table.SSTableMetadata;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;


public class MetadataJsonRW extends JsonFile {

    public final static String METADATA_FILE_NAME = "metadata.json";

    private final File ssTableMetadataJson;

    public MetadataJsonRW(Path dirPath) {
        this(dirPath.toFile());
    }

    public MetadataJsonRW(File dirPath) {
        this.ssTableMetadataJson = new File(dirPath, METADATA_FILE_NAME);
    }

    public SSTableMetadata read() {
        try {
            return objectMapper.readValue(ssTableMetadataJson, SSTableMetadata.class);
        } catch (IOException e) {
            throw new KVStoreIOException("Cannot read " + METADATA_FILE_NAME + " from path " + ssTableMetadataJson.getAbsolutePath(), e);
        }

    }

    public void write(SSTableMetadata ssTableMetadata) {
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            objectMapper.writeValue(ssTableMetadataJson, ssTableMetadata);
        } catch (IOException e) {
            throw new KVStoreIOException("Cannot write " + METADATA_FILE_NAME + " to " + ssTableMetadataJson.getAbsolutePath(), e);
        }
    }

}
