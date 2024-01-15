package com.keks.kv_storage.bplus.tree.node.key;


import java.util.Objects;


public class KeyLocationAndRightChild {

    public final KeyLocation keyLocation;
    public final long rightChild;

    public KeyLocationAndRightChild(KeyLocation keyLocation, long rightChild) {
        this.keyLocation = keyLocation;
        this.rightChild = rightChild;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        KeyLocationAndRightChild that = (KeyLocationAndRightChild) o;
        return rightChild == that.rightChild && Objects.equals(keyLocation, that.keyLocation);
    }

    @Override
    public int hashCode() {
        return Objects.hash(keyLocation, rightChild);
    }
}
