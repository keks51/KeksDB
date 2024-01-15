package com.keks.kv_storage.bplus.tree.node.traverse;

import com.keks.kv_storage.bplus.buffer.CachedPageNew;
import com.keks.kv_storage.bplus.tree.node.leaf.TreeLeafNode;


public class TraverseLeafNode extends TraverseItem<TreeLeafNode> {
    public TraverseLeafNode(CachedPageNew<TreeLeafNode> nodePage) {
        super(nodePage);
    }

    public TraverseLeafNode(CachedPageNew<TreeLeafNode> nodePage,
                            CachedPageNew<TreeLeafNode> leftSiblingPage,
                            CachedPageNew<TreeLeafNode> rightSiblingPage) {
        super(nodePage, leftSiblingPage, rightSiblingPage);
    }


}
