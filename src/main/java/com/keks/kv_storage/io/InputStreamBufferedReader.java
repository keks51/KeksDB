package com.keks.kv_storage.io;

import com.keks.kv_storage.record.KVRecord;
import com.keks.kv_storage.record.KvRow;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;


public class InputStreamBufferedReader extends BufferedReader<KVRecord> { // TODO test it with records bigger then bufsize

    private final InputStream is;
    public InputStreamBufferedReader(InputStream is, int bufSize, int thresholdToUpdateBuf) {
        super(bufSize, thresholdToUpdateBuf);
        this.is = is;
    }

    protected void readToBuffer(ByteBuffer buffer) {
        try {
            // sometimes update can be triggered when not all bytes were read.
            // if buff contains not read bytes then shift them to buf head
            int remaining = buffer.remaining();
            if (remaining > 0) {
                byte[] bytesToMoveHead = new byte[remaining];
                buffer.get(bytesToMoveHead);
                buffer.clear();
                buffer.put(bytesToMoveHead);
            } else {
                buffer.clear();
            }

            // read bytes until read != -1 or buffer is full (buffer.remaining() == 0)
            int bytesInBuffer = remaining;
            do {
                int read = is.read(buffer.array(), bytesInBuffer, buffer.remaining());
                if (read == -1) {
                    break;
                } else {
                    bytesInBuffer += read;
                    buffer.position(bytesInBuffer);
                }
            } while (buffer.remaining() > 0);

            // sometimes buf is not full after reading. bytesInBuffer != bufSize and remaining > 0.
            // For such situation we set remaining to 0 by limit
            buffer.clear();
            buffer.limit(bytesInBuffer);
            buffer.mark();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    protected KVRecord buildRecord(ByteBuffer bb) {
        KvRow kvRow = KvRow.fromByteBuffer(bb);
        return new KVRecord(kvRow.keyBytes[0], kvRow.valueBytes[0]);
//        return KVRecord.fromByteBuffer(bb);
    }

    protected int calcRecordLen(ByteBuffer bb) {
        try {
            bb.mark();
            return bb.getInt();
        }finally {
            bb.reset();
        }
    }

}
