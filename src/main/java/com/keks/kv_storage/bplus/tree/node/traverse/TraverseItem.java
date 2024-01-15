package com.keks.kv_storage.bplus.tree.node.traverse;

import com.keks.kv_storage.bplus.buffer.CachedPageNew;
import com.keks.kv_storage.bplus.tree.node.TreeNode;


public abstract class TraverseItem<T extends TreeNode<?>> {

    public final CachedPageNew<T> nodePage;
    public CachedPageNew<T> leftSiblingPage;
    public CachedPageNew<T> rightSiblingPage;

    public TraverseItem(CachedPageNew<T> nodePage) {
        this.nodePage = nodePage;
    }

    public TraverseItem(CachedPageNew<T> nodePage,
                            CachedPageNew<T> leftSiblingPage,
                            CachedPageNew<T> rightSiblingPage) {
        this.nodePage = nodePage;
        this.leftSiblingPage = leftSiblingPage;
        this.rightSiblingPage = rightSiblingPage;
    }

    public CachedPageNew<T> getLeftSiblingPage() {
        return leftSiblingPage;
    }

    public CachedPageNew<T> getRightSiblingPage() {
        return rightSiblingPage;
    }

}
