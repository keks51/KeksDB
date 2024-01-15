package com.keks.kv_storage.bplus.page_manager.pageio;


import com.keks.kv_storage.bplus.page_manager.Page;
import com.keks.kv_storage.bplus.page_manager.PageIO;
import com.keks.kv_storage.bplus.page_manager.page_disk.sp.SlottedPage;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;


public class SlottedPageIO extends PageIO<SlottedPage> {

    public final AtomicInteger read = new AtomicInteger();
    public final AtomicInteger write = new AtomicInteger();

    public SlottedPageIO(File file, String fileName) throws IOException {
        super(file, fileName, SlottedPage.DEFAULT_PAGE_SIZE);
    }

    public SlottedPage getPage(long pageId) throws IOException {
        read.incrementAndGet();
        ByteBuffer bb = readBB(pageId);
        if (bb.getShort() == 0) { // if page is doesn't exist)
            return new SlottedPage(pageId);
        } else {
            bb.position(0);
            return new SlottedPage(bb);
        }
    }

    @Override
    public void flush(Page page) throws IOException {
        write.incrementAndGet();
        flushByteBuffer(page.pageId, ByteBuffer.wrap(page.toBytes()));
    }

    private ArrayList<SlottedPage> forceLoadMultiplyPage(long startPage, int number) throws IOException {
        byte[] bytesToLoad = new byte[number * SlottedPage.DEFAULT_PAGE_SIZE];
        ByteBuffer bb = ByteBuffer.wrap(bytesToLoad);

        bb.position(0);
        ArrayList<SlottedPage> pages = new ArrayList<>(number);
        for (int page = 0; page < number; page++) {
            bb.clear();
            bb.position(page * SlottedPage.DEFAULT_PAGE_SIZE);
            bb.mark();
            short pageType = bb.getShort();
            bb.reset();
            if (pageType == 0) { // if page is doesn't exist)
                pages.add(new SlottedPage(startPage + page));
            } else {
                SlottedPage slottedPage = new SlottedPage(bb);
                pages.add(slottedPage);
            }
        }
        return pages;
    }

    public long getPagesCnt() throws IOException {
        return getFileSize() / SlottedPage.DEFAULT_PAGE_SIZE;
    }

    public Iterator<SlottedPage> getAllPages() throws IOException {
        return new Iterator<>() {
            long curPage = 0;
            int cursor = -1;
            final int maxPagesToLoad = 64;
            final long lastAvailablePage = getFileSize() / SlottedPage.DEFAULT_PAGE_SIZE;
            ArrayList<SlottedPage> loadedPages = forceLoadMultiplyPage(0, maxPagesToLoad);

            @Override
            public boolean hasNext() {
                return curPage < lastAvailablePage;
            }

            @Override
            public SlottedPage next() {
                if (cursor + 1 == maxPagesToLoad) {
                    try {
                        loadedPages = forceLoadMultiplyPage(curPage, maxPagesToLoad);
                        cursor = -1;
                    } catch (IOException e) {
                        e.printStackTrace();
                        throw new RuntimeException(e);
                    }
                }
                curPage++;
                cursor++;
                return loadedPages.get(cursor);
            }
        };
    }

}
