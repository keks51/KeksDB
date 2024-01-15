package com.keks.kv_storage.bplus.item;


public interface TupleSplitter {

    boolean hasNext();

    KvRecordSplit next(int size);

    int getLeftToWrite();

}
