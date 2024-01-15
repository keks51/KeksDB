package com.keks.kv_storage.bplus.page_manager;

//import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;


public abstract class PageIO<T extends Page> {

//    final static Logger logger = Logger.getLogger(PageIO.class);

    private final FileChannel channel;

    private final int pageSize;

    public PageIO(File tableDir, String fileName,
                  int pageSize) throws IOException {
        File pageFile = new File(tableDir, fileName);
        if (!pageFile.exists()) pageFile.createNewFile();
        this.pageSize = pageSize;
        this.channel = FileChannel.open(new File(tableDir, fileName).toPath(),
                StandardOpenOption.SYNC, // DSYNC not always persist data
                StandardOpenOption.WRITE,
                StandardOpenOption.READ);
    }

    public ByteBuffer readBB(long pageId) throws IOException {
        printThread("read " + pageId);
        byte[] bytes = new byte[pageSize];
        ByteBuffer bb = ByteBuffer.wrap(bytes);
        channel.read(bb, pageId * pageSize);
        bb.position(0);
        return bb;
    }

    public abstract T getPage(long pageId) throws IOException;

    public void flushByteBuffer(long pageId, ByteBuffer bb) throws IOException {
        printThread("flushed " + pageId);
        channel.write(bb, pageId * pageSize);
    }

    public void flush(Page page) throws IOException {
        flushByteBuffer(page.pageId, ByteBuffer.wrap(page.toBytes()));
    }

    public void close() throws IOException {
        channel.force(true);
        channel.close();
    }

    public long getFileSize() throws IOException {
        return channel.size();
    }

    public static void printThread(String msg) {
//        System.out.println("Thread[" + Thread.currentThread().getId() + "]  PageIO: " + msg);
//        logger.debug("Thread[" + Thread.currentThread().getId() + "] " + msg);
    }

}
