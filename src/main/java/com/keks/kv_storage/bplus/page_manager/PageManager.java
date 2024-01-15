package com.keks.kv_storage.bplus.page_manager;


import com.keks.kv_storage.bplus.buffer.CachedPageNew;

import java.io.IOException;


public abstract class PageManager<T extends Page> {

    public abstract CachedPageNew<T> getAndLockWriteFreePage() throws IOException;

    public abstract CachedPageNew<T> getAndLockWritePage(long pageId) throws IOException;

    public abstract CachedPageNew<T> getAndLockReadPage(long pageId) throws IOException;

    public abstract int isPageFree(long pageId);

    public abstract void setForceNotFreePage(long pageId);

    public abstract void unlockPage(CachedPageNew<T> cachedPage) throws IOException;

    public abstract String printFreeSpaceInfo();

    public abstract void close() throws IOException;

}
