package com.keks.kv_storage.io;

import com.keks.kv_storage.Item;
import com.keks.kv_storage.TypeSize;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;


class BufferedReaderTest {

    static class StringRecord implements Item {

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
        private final int len;
        private final String record;

        public StringRecord(String record) {
            this.record = record;
            this.len = TypeSize.INT + record.length();
        }

        public StringRecord(ByteBuffer bb) {
            this.len = bb.getInt();
            byte[] bytes = new byte[len - TypeSize.INT];
            bb.get(bytes);
            this.record = new String(bytes);
        }

        @Override
        public int getLen() {
            return len;
        }

        @Override
        public void copyToBB(ByteBuffer bb) {
            bb.putInt(len);
            bb.put(record.getBytes());
        }

        @Override
        public byte[] getBytes() {
            ByteBuffer allocate = ByteBuffer.allocate(len);
            copyToBB(allocate);
            return allocate.array();
        }
    }

    static class MyBufferedReader extends FileChannelBufferedReader<StringRecord> {

        public MyBufferedReader(FileChannel channel, long filePos, int bufSize) {
            super(channel, filePos, bufSize, TypeSize.INT);
        }

        @Override
        protected StringRecord buildRecord(ByteBuffer bb) {
            return new StringRecord(bb);
        }

        @Override
        protected int calcRecordLen(ByteBuffer bb) {
            try {
                bb.mark();
                return bb.getInt();
            }finally {
                bb.reset();
            }
        }

    }

    @Test
    public void testBufferedReaderEmptyFile(@TempDir Path dir) throws IOException {
        FileChannel channel = FileChannel.open(
                new File(dir.toFile(), "test-file").toPath(),
                StandardOpenOption.READ,
                StandardOpenOption.WRITE,
                StandardOpenOption.CREATE_NEW);
        int buffSize = 50;
        channel.force(true);

        MyBufferedReader myBufferedReader = new MyBufferedReader(channel, 0, buffSize);
        
        assert !myBufferedReader.hasNext();
    }


    @Test
    public void testBufferedReader(@TempDir Path dir) throws IOException {

        FileChannel channel = FileChannel.open(
                new File(dir.toFile(), "test-file").toPath(),
                StandardOpenOption.READ,
                StandardOpenOption.WRITE,
                StandardOpenOption.CREATE_NEW);
        int buffSize = 50;

        String data = "Lorem \n" +
                "ipsum \n" +
                "dolor sit amet, \n" +
                "consectetur adipiscing elit, sed do eiusmod tempor \n" +
                "incididunt \n" +
                "ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex \n" +
                "ea commodo consequat. Duis aute irure \n" +
                "dolor in \n" +
                "reprehenderit \n" +
                "\n" +
                "in voluptate velit esse cillum dolore eu \n" +
                "line contains 50 chars includingRecordOverhead\n" + // 66
                "line contains 100 chars includingRecordOverhead aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\n" + //116
                "line contains 150 chars includingRecordOverhead aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\n" + // 166
                "line contains 170 chars includingRecordOverhead aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\n" + //186
                "fugiat nulla pariatur. Excepteur sint occaecat \n" +
                "cupidatat non \n" +
                "proident, \n" +
                "sunt in\n" +
                " culpa qui officia deserunt mollit anim id est";

        String[] strings = data.split("\n");
        for (String string : strings) {
            StringRecord stringRecord = new StringRecord(string);
            ByteBuffer bb = ByteBuffer.allocateDirect(stringRecord.getLen());
            stringRecord.copyToBB(bb);
            bb.clear();
            channel.write(bb);
        }
        channel.force(true);

        MyBufferedReader myBufferedReader = new MyBufferedReader(channel, 0, buffSize);
        while (myBufferedReader.hasNext()) {
            StringRecord next = myBufferedReader.next();
            System.out.println(next.record);
            if (next.record.contains("dolor sit amet")) {
                System.out.println();
            }
        }

    }

}