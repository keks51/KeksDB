package com.keks.kv_storage.record;

import com.keks.kv_storage.Item;
import com.keks.kv_storage.TypeSize;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;


// TODO set key and value columns limitation Short.MAX_VALUE
public class KvRow implements Item {

    public final short keyDataElemsNum;
    public final short valueDataElemsNum;

    public final byte[][] keyBytes;
    public final byte[][] valueBytes;
    private final int totalRowLen;

    public KvRow(int keyArrDataLen, int valueArrDataLen, byte[][] key, byte[][] value) {
        this.keyBytes = key;
        this.valueBytes = value;
        this.keyDataElemsNum = (short) keyBytes.length;
        this.valueDataElemsNum = (short) valueBytes.length;
        this.totalRowLen =
                TypeSize.INT // total len
                        + TypeSize.SHORT // key number of elems
                        + keyArrDataLen // key arr data len
                        + TypeSize.SHORT // value number of elems
                        + valueArrDataLen; // value arr data len
    }

    @Override
    public void copyToBB(ByteBuffer bb) {
        bb.putInt(totalRowLen);

        bb.putShort(keyDataElemsNum);
        for (byte[] keyByte : keyBytes) {
            bb.putShort((short) keyByte.length);
            bb.put(keyByte);
        }

        bb.putShort(valueDataElemsNum);
        for (byte[] valueByte : valueBytes) {
            bb.putInt(valueByte.length);
            bb.put(valueByte);
        }
    }

    public static KvRow fromByteBuffer(ByteBuffer bb) {
        int len = bb.getInt();
        if (bb.remaining() + TypeSize.INT < len) throw new RuntimeException();


        short keyDataElemsNum = bb.getShort();
        byte[][] key = new byte[keyDataElemsNum][];
        int keyArrDataLen = 0;
        for (int i = 0; i < keyDataElemsNum; i++) {
            short keyLen = bb.getShort();
            byte[] bytes = new byte[keyLen];
            bb.get(bytes);
            key[i] = bytes;
//            System.out.println("decoded key: " + new String(bytes));
            keyArrDataLen += TypeSize.SHORT + keyLen;
        }


        int valueArrDataLen = 0;
        short valueDataElemsNum = bb.getShort();
        byte[][] value = new byte[valueDataElemsNum][];
        {
            for (int i = 0; i < valueDataElemsNum; i++) {
                int valueLen = bb.getInt();
                byte[] bytes = new byte[valueLen];
                bb.get(bytes);
                value[i] = bytes;
//                System.out.println("decoded value: " + new String(bytes));
                valueArrDataLen += TypeSize.INT + valueLen;
            }

        }

        return new KvRow(keyArrDataLen, valueArrDataLen, key, value);
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
        return totalRowLen;
    }

    @Override
    public byte[] getBytes() {
        ByteBuffer bb = ByteBuffer.allocate(totalRowLen);
        copyToBB(bb);
        return bb.array();
    }

    public boolean isDeleted() {
        return valueDataElemsNum == 0;
    }



    //[key1]
    //[key_sec_2]
    //[value_1]
    //[value_test]
    //
    public static KvRow of(byte[][] key, byte[][] value) {
        int keyArrDataLen = 0;
        for (byte[] keyByte : key) {
            keyArrDataLen += TypeSize.SHORT + keyByte.length;
        }

        int valueArrDataLen = 0;
        for (byte[] valueByte : value) {
            valueArrDataLen += TypeSize.INT + valueByte.length;
        }

        return new KvRow(keyArrDataLen, valueArrDataLen, key, value);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        KvRow kvRow = (KvRow) o;
        return keyDataElemsNum == kvRow.keyDataElemsNum
                && valueDataElemsNum == kvRow.valueDataElemsNum
                && totalRowLen == kvRow.totalRowLen
                && Arrays.deepEquals(keyBytes, kvRow.keyBytes)
                && Arrays.deepEquals(valueBytes, kvRow.valueBytes);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(keyDataElemsNum, valueDataElemsNum, totalRowLen);
        result = 31 * result + Arrays.deepHashCode(keyBytes);
        result = 31 * result + Arrays.deepHashCode(valueBytes);
        return result;
    }
}
