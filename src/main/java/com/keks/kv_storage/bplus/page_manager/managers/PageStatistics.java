package com.keks.kv_storage.bplus.page_manager.managers;


public class PageStatistics {

    private final long pageId;
    private final int itemsNumber;
    private final boolean isFree;
    private final long availableSpacesInBytes;

    public PageStatistics(long pageId, int itemsNumber, boolean isFree, long availableSpacesInBytes) {
        this.pageId = pageId;
        this.itemsNumber = itemsNumber;
        this.isFree = isFree;
        this.availableSpacesInBytes = availableSpacesInBytes;
    }

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer("PageStatistics(");
        sb.append("pageId=").append(pageId);
        sb.append(", itemsNumber=").append(itemsNumber);
        sb.append(", isFree=").append(isFree);
        sb.append(", availableSpacesInBytes=").append(availableSpacesInBytes);
        sb.append(')');
        return sb.toString();
    }
}
