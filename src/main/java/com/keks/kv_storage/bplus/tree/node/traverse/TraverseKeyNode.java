package com.keks.kv_storage.bplus.tree.node.traverse;

import com.keks.kv_storage.bplus.buffer.CachedPageNew;
import com.keks.kv_storage.bplus.tree.node.key.TreeKeyNode;


public class TraverseKeyNode extends TraverseItem<TreeKeyNode> {

    public final boolean childIsLeaf;

    public TraverseKeyNode(CachedPageNew<TreeKeyNode> nodePage,
                           boolean childIsLeaf) {
       super(nodePage);
        this.childIsLeaf = childIsLeaf;
    }

    public TraverseKeyNode(CachedPageNew<TreeKeyNode> nodePage,
                           CachedPageNew<TreeKeyNode> leftSiblingPage,
                           CachedPageNew<TreeKeyNode> rightSiblingPage,
                           boolean childIsLeaf) {
       super(nodePage, leftSiblingPage, rightSiblingPage);
        this.childIsLeaf = childIsLeaf;
    }

}
