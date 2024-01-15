package com.keks.kv_storage.bplus.page_manager;

import com.keks.kv_storage.bplus.page_manager.page_key.PageType;

import java.util.Objects;


public class PageKey {

    public final PageType pageType;
    public final long pageId;

    public PageKey(PageType pageType, long pageId) {
        this.pageType = pageType;
        this.pageId = pageId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PageKey pageKey = (PageKey) o;
        return pageType == pageKey.pageType && pageId == pageKey.pageId;
    }

    @Override
    public int hashCode() {
        return Objects.hash(pageType, pageId);
    }


    public PageKey copy(long newPageId) {
        return new PageKey(pageType, newPageId);
    }

    @Override
    public String toString() {
        return "PageKey(" + pageId + ", " + pageType + ")";
    }

}
