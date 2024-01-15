package com.keks.kv_storage.lsm.query;

import com.keks.kv_storage.record.KVRecord;
import com.keks.kv_storage.query.EngineIterator;

import java.util.*;


public class LsmRecordsIterator extends EngineIterator {

    private final PriorityQueue<RecordsIter> queue;

    private KVRecord curElm;

    private KVRecord nextElem;

    // order of input arr is strict since if 2 records have the same key then the correct elem is which one closer to head
    public LsmRecordsIterator(List<SsTableRangeIterator> ssTableIterators) {
        PriorityQueue<RecordsIter> itersQueue = new PriorityQueue<>();
        for (int i = 0; i < ssTableIterators.size(); i++) {
            SsTableRangeIterator iterator = ssTableIterators.get(i);
            if (iterator.hasNext()) {
                RecordsIter entry = new RecordsIter(i, iterator.next(), iterator);
                itersQueue.add(entry);
            }
        }
        this.queue = itersQueue;
        this.curElm = getNextRecord();
        this.nextElem = getNextRecord();
    }

    private KVRecord getNextRecord() {
        RecordsIter recordsIter = queue.poll();
        if (recordsIter == null) {
            return null;
        } else {
            KVRecord record = recordsIter.getRecord();
            if (recordsIter.hasNextElement()) {
                recordsIter.roll();
                queue.add(recordsIter);
            }
            return record;
        }
    }

    @Override
    protected boolean hasNextCloseable() {
        while (curElm != null) {
            if (nextElem == null) {
                if (curElm.isDeleted()) {
                    curElm = null;
                    return false;
                } else {
                    return true;
                }
            }

            if (curElm.key.equals(nextElem.key)) {
                nextElem = getNextRecord();
            } else {
                if (curElm.isDeleted()) {
                    curElm = nextElem;
                    nextElem = getNextRecord();
                } else {
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    public KVRecord next() {
        KVRecord res = curElm;
        curElm = nextElem;
        nextElem = getNextRecord();
        return res;
    }

    @Override
    public void close() throws Exception {
        queue.forEach(e -> e.ssTableRangeIterator.close());
    }

}

class RecordsIter implements Comparable<RecordsIter> {

    private final int order;

    private KVRecord elem;

    public final SsTableRangeIterator ssTableRangeIterator;

    public RecordsIter(int order, KVRecord elem, SsTableRangeIterator ssTableRangeIterator) {
        this.order = order;
        this.elem = elem;
        this.ssTableRangeIterator = ssTableRangeIterator;
    }

    public KVRecord getRecord() {
        return elem;
    }

    public boolean hasNextElement() {
        return ssTableRangeIterator.hasNext();
    }

    public void roll() {
        this.elem = ssTableRangeIterator.next();
    }

    @Override
    public int compareTo(RecordsIter o) {
        int i = elem.key.compareTo(o.getRecord().key);
        if (i == 0) return (order < o.order) ? -1 : 1;
        return i;
    }

}
