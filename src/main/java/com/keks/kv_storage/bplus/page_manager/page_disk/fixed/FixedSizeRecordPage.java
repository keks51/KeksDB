package com.keks.kv_storage.bplus.page_manager.page_disk.fixed;


import java.nio.ByteBuffer;

// store all in byte[SIZE] to reduce memory overhead of storing several byte[]
public class FixedSizeRecordPage {

    public static int SIZE = 8 * 1024; // 4_096
    public static final int SIZE_INT = 4;
    public static final int SIZE_LONG = 8;
    public static int AVAILABLE_SIZE = SIZE - SIZE_LONG - SIZE_INT - SIZE_LONG;
    private final BitMask bitMask;
    public final long pageId;
    private int nextRecordSlotId;
    private final int RECORD_LEN = 100;
    private int recordsCnt;
    private final int maxRecordsCnt = (AVAILABLE_SIZE / RECORD_LEN);
    private final int dataLen = maxRecordsCnt * RECORD_LEN;
    private final byte[] data = new byte[dataLen];

    public FixedSizeRecordPage(int pageId) {
        this.pageId = pageId;
        this.nextRecordSlotId = 0;
        this.bitMask = new BitMask();
        this.recordsCnt = 0;
    }

    public FixedSizeRecordPage(ByteBuffer bb) {
        this.pageId = bb.getLong();
        this.recordsCnt = bb.getInt();
        this.bitMask = new BitMask(bb.getLong());
        bb.get(this.data);
    }

    public int getRecordsCount() {
        return recordsCnt;
    }

    public boolean isFull() {
        return recordsCnt >= maxRecordsCnt;
    }

    public boolean isAlmostFull() {
        return recordsCnt  >= maxRecordsCnt - 1;
    }

    // TODO check read empty String   write("")  readUTF
    public int add(byte[] newRecord) {
        if (isFull()) throw new RuntimeException("No space left in page");
        int slotToSet = nextRecordSlotId;
        int startPos = slotToSet * RECORD_LEN;
        System.arraycopy(newRecord, 0, data, startPos, Math.min(newRecord.length, RECORD_LEN));
        bitMask.setBit(slotToSet);
        nextRecordSlotId = bitMask.nextUnsetBit();
        recordsCnt++;
        return slotToSet;
    }

    public void update(int startPos, byte[] newRecord) {
        int pos = startPos * RECORD_LEN;
        System.arraycopy(newRecord, 0, data, pos, newRecord.length);
    }

    public void delete(int slotId) {
        bitMask.unSetBit(slotId);
        nextRecordSlotId = slotId;
        recordsCnt--;
    }

    public byte[] getRecord(int slotId) {
        byte[] bytes = new byte[RECORD_LEN];
        int pos = slotId * RECORD_LEN;
        System.arraycopy(data, pos, bytes, 0, RECORD_LEN);
        return bytes;
    }

    public String getBitMask() {
        return bitMask.toString();
    }

    public byte[] toBytes() {
        byte[] bytes = new byte[SIZE];
        ByteBuffer bb = ByteBuffer.wrap(bytes);
        bb.putLong(pageId);
        bb.putInt(recordsCnt);
        bb.putLong(bitMask.b);
        bb.put(data);
        return bytes;
    }

    static class BitMask {

        private long b;

        public BitMask() {
            this(0L);
        }

        public BitMask(long b) {
            this.b = b;
        }

        public void setBit(int pos) {
            long o = 1L << pos;
            b = b | o;
        }

        public void unSetBit(int pos) {
            long o = 1L << pos;
            b = b ^ o;
        }

        public void print() {
            System.out.println(Long.toBinaryString(b));
            System.out.println();
        }


        public boolean isSet(int pos) {
            long o = 1L << pos;
            long t = b & o;
            return t != 0;
        }

        public int nextUnsetBit() {
            return getFirstUnsetFromRight(b);
        }

        public static int getFirstUnsetFromRight(long n) {
            if (n == -1) { // all bits are set 11111.....111
                return -1;
            } else {
                return (int) ((Math.log10((n & ~(n + 1)) + 1) / Math.log10(2))); // log10(16)/log10(2) => log2(16)
            }
        }

        @Override
        public String toString() {
            return Long.toBinaryString(b);
        }
    }



}
