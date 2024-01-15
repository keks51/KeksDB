package com.keks.kv_storage.recovery;

import java.nio.ByteBuffer;

public enum JournalEvent {

    CREATING_TABLE,
    RUNNING,
    RECOVERING_DATA_FROM_CHECKPOINT,
    CREATING_CHECKPOINT,
    END;

    private final ByteBuffer bb;

    JournalEvent() {
        ByteBuffer byteBuffer = ByteBuffer.allocateDirect(this.name().getBytes().length);
        byteBuffer.put(this.name().getBytes());
        bb = byteBuffer;
    }

    public ByteBuffer getBB() {
        bb.clear();
        return bb;
    }
}