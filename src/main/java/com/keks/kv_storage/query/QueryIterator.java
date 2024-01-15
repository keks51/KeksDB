package com.keks.kv_storage.query;

import com.keks.kv_storage.record.KVRecord;


public class QueryIterator extends AutoCloseableIterator {

    private int readCnt = 0;

    private final AutoCloseableIterator engineIterator;
    private final Query query;

    public QueryIterator(AutoCloseableIterator engineIterator, Query query) {
        this.engineIterator = engineIterator;
        this.query = query;
    }

    @Override
    public void close() {
        try {
            engineIterator.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected boolean hasNextCloseable() {
        if (readCnt >= query.limit) {
            return false;
        }
        return engineIterator.hasNext();
    }

    @Override
    public KVRecord next() {
        if (query.limit > readCnt) {
            readCnt++;
            return engineIterator.next();
        } else {
            return null;
        }

    }

}
