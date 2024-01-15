package com.keks.kv_storage.lsm.io;

import com.keks.kv_storage.lsm.conf.LsmConf;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;


class LsmConfJsonRWTest {

    @Test
    public void testReadWrite(@TempDir Path tmpPath) throws IOException {
        LsmConf lsmConfExp = new LsmConf(10, 15, 0.5);
        try(LsmConfJsonRW lsmConfJsonRW = new LsmConfJsonRW(tmpPath)) {
            lsmConfJsonRW.write(lsmConfExp);
        }

        try(LsmConfJsonRW lsmConfJsonRW = new LsmConfJsonRW(tmpPath)) {
            LsmConf lsmConfAct  = lsmConfJsonRW.read();
            assertEquals(lsmConfExp, lsmConfAct);
        }
    }

}


