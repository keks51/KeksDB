package com.keks.kv_storage.bplus.tree.node.traverse;

import java.util.Stack;


public class TraverseNodes {

    public final Stack<TraverseKeyNode> keyNodesStack;

    public final TraverseLeafNode leafNode;

    public TraverseNodes(Stack<TraverseKeyNode> keyNodesStack, TraverseLeafNode leafNode) {
        this.keyNodesStack = keyNodesStack;
        this.leafNode = leafNode;
    }

}
