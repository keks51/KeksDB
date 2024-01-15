package com.keks.kv_storage;

import java.nio.ByteBuffer;


public interface Item {

    int getMinSize();

    int getTotalLen(ByteBuffer bb);

    int getLen();

    void copyToBB(ByteBuffer bb);

    byte[] getBytes();

}
