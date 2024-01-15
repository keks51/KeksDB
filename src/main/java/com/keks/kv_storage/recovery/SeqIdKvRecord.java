package com.keks.kv_storage.recovery;

import com.keks.kv_storage.Item;
import com.keks.kv_storage.TypeSize;
import com.keks.kv_storage.record.KVRecord;
import com.keks.kv_storage.record.KvRow;

import java.nio.ByteBuffer;


public class SeqIdKvRecord implements Comparable<SeqIdKvRecord>, Item {

    public long id;
    public KVRecord kvRecord;

    public final int len;

    public SeqIdKvRecord(long id, KVRecord kvRecord) {
        this.id = id;
        this.kvRecord = kvRecord;
        this.len = TypeSize.INT + TypeSize.LONG + kvRecord.getLen();
    }

    public static SeqIdKvRecord fromByteBuffer(ByteBuffer bb) {
        int len = bb.getInt();
        if (bb.remaining() + TypeSize.INT < len) throw new RuntimeException();
        long seqId = bb.getLong();
        KvRow kvRow = KvRow.fromByteBuffer(bb);
        return new SeqIdKvRecord(seqId,  new KVRecord(kvRow.keyBytes[0], kvRow.valueBytes[0]));
    }

    @Override
    public int compareTo(SeqIdKvRecord o) {
        int i = Long.compare(id, o.id);
//            System.out.println("Compare: " + id + " " + o.id + " res: " + i);
        assert i != 0;
        return i;
    }

    @Override
    public int getMinSize() {
        return TypeSize.INT;
    }

    @Override
    public int getTotalLen(ByteBuffer bb) {
        try {
            bb.mark();
            return bb.getInt();
        } finally {
            bb.reset();
        }
    }

    @Override
    public int getLen() {
        return len;
    }

    @Override
    public void copyToBB(ByteBuffer bb) {
        bb.putInt(len);
        bb.putLong(id);
        bb.put(kvRecord.getBytes());
    }

    @Override
    public byte[] getBytes() {
        ByteBuffer bb = ByteBuffer.allocate(len);
        copyToBB(bb);
        return bb.array();
    }
}
