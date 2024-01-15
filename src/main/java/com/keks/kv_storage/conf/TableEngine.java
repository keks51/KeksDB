package com.keks.kv_storage.conf;

import com.keks.kv_storage.record.KVRecord;
import com.keks.kv_storage.query.Query;
import com.keks.kv_storage.query.QueryIterator;

import java.io.IOException;


public abstract class TableEngine {

    public abstract void putKV(byte[] key, byte[] value) throws IOException;

    public abstract void put(KVRecord kvRecord) throws IOException;

    public abstract void remove(String key) throws IOException;

    public abstract KVRecord get(String key) throws IOException;

    public  abstract void close() throws IOException;

    public abstract void forceFlush() throws IOException;

    public abstract void optimize() throws IOException;

    public abstract Params<?> getTableProperties();

    public abstract long getRecordsCnt(Query query) throws IOException;

    public abstract QueryIterator getRangeRecords(Query query) throws IOException;

}
