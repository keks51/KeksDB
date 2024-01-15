package com.keks.kv_storage.lsm.query;

import com.keks.kv_storage.record.KVRecord;
import com.keks.kv_storage.query.*;
import com.keks.kv_storage.query.range.MaxRangeKey;
import com.keks.kv_storage.query.range.MinRangeKey;
import com.keks.kv_storage.query.range.RangeKey;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;


public class SsTableRangeIterator extends AutoCloseableIterator {

    public static final SsTableRangeIterator EMPTY = new SsTableRangeIterator(
            Collections.emptyIterator(), new MinRangeKey(), new MaxRangeKey(), () -> {});


    private final RangeKey leftBound;
    private final RangeKey right;
    private final Runnable onCloseAction;

    private final Iterator<KVRecord> kvRecordIterator;

    private KVRecord nextRecord;

    private boolean wasClosed = false;

    public SsTableRangeIterator(Iterator<KVRecord> kvRecordIterator,
                                RangeKey leftBound,
                                RangeKey right,
                                Runnable onCloseAction) {
        this.leftBound = leftBound;
        this.right = right;
        this.onCloseAction = onCloseAction;
        this.kvRecordIterator = kvRecordIterator;
        rollToFirstLeftRecord();
    }

    // since record from disk can be lower than left (record=key05 and left=key08)
    // we have to skip records till eq or greater
    private void rollToFirstLeftRecord() {
        if (kvRecordIterator.hasNext()) {
            KVRecord leftRecord = kvRecordIterator.next();
            while (leftBound.isGreater(leftRecord.key) && kvRecordIterator.hasNext()) {
                leftRecord = kvRecordIterator.next();
            }

            if (leftBound.isGreater(leftRecord.key) && !kvRecordIterator.hasNext()) return;

            if (leftBound.isExclusive() && leftBound.isEqual(leftRecord.key)) {
                if (kvRecordIterator.hasNext()) {
                    leftRecord = kvRecordIterator.next();
                } else {
                    leftRecord = null;
                }
            }
            nextRecord = leftRecord;
        }

        if (nextRecord != null) {
            if (right.isLower(nextRecord.key)
                    || (right.isEqual(nextRecord.key) && right.isExclusive())) {
                nextRecord = null;
            }
        }
    }


    private void rollToNextRecord() {
        if (kvRecordIterator.hasNext()) {
            KVRecord record = kvRecordIterator.next();
            if (right.isGreater(record.key) || (right.isInclusive()) && right.isEqual(record.key)) {
                nextRecord = record;
            } else {
                nextRecord = null;
            }
        } else {
            nextRecord = null;
        }
    }


    @Override
    protected boolean hasNextCloseable() {
        return nextRecord != null;
    }

    @Override
    public KVRecord next() {
        KVRecord returnValue = nextRecord;
        rollToNextRecord();
        return returnValue;
    }

    public void close() {
        if (!wasClosed) {
            wasClosed = true;
            onCloseAction.run();
        }
    }

    public ArrayList<KVRecord> getAsArr() {
        ArrayList<KVRecord> list = new ArrayList<>();
        while (hasNext()) {
            list.add(next());
        }
        return list;
    }

}
