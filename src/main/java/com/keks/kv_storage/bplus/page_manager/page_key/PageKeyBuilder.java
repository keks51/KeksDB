package com.keks.kv_storage.bplus.page_manager.page_key;


import com.keks.kv_storage.bplus.page_manager.PageKey;

public class PageKeyBuilder {

    public final PageType pageType;

    public PageKeyBuilder(PageType pageType) {

        this.pageType = pageType;
    }

    public PageKey getPageKey(long pageId) {
        return new PageKey(pageType, pageId);
    }

}
