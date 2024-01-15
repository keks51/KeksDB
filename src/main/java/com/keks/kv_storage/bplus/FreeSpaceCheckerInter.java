package com.keks.kv_storage.bplus;

import java.io.IOException;

public interface FreeSpaceCheckerInter {

    int isFree(long pageId);

    long nextFreePage() throws IOException;

    boolean tryToTakePage(long pageId);

    void setForceNotFree(long pageId);

    void setFree(long pageId);

    void close() throws IOException;


}
