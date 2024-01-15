package com.keks.kv_storage.lsm.io;

import com.keks.kv_storage.lsm.ss_table.SSTableMetadata;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;


class MetadataJsonRWTest {

    @Test
    public void testRadWrite(@TempDir Path tmpPath) throws IOException {
        SSTableMetadata ssTableMetadata = new SSTableMetadata(20, 5, 4);
        try (MetadataJsonRW metadataJsonRW = new MetadataJsonRW(tmpPath)) {
            metadataJsonRW.write(ssTableMetadata);
        }

        try (MetadataJsonRW metadataJsonRW = new MetadataJsonRW(tmpPath)) {
            SSTableMetadata ssTableMetadataFromDisk = metadataJsonRW.read();
            assertEquals(ssTableMetadata, ssTableMetadataFromDisk);
        }
    }

}