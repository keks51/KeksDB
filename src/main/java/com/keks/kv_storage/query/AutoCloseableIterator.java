package com.keks.kv_storage.query;

import com.keks.kv_storage.record.KVRecord;

import java.util.ArrayList;
import java.util.Iterator;


public abstract class AutoCloseableIterator implements Iterator<KVRecord>, AutoCloseable {

    private boolean wasClosed = false;

    public ArrayList<KVRecord> getAsArr() {
        ArrayList<KVRecord> list = new ArrayList<>();
        while (hasNext()) {
            list.add(next());
        }
        return list;
    }

    @Override
    public boolean hasNext() {
        if (hasNextCloseable()) {
            return true;
        } else {
            try {
                if (!wasClosed) {
                    wasClosed = true;
                    close();
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            return false;
        }
    }

    protected abstract boolean hasNextCloseable();

}
