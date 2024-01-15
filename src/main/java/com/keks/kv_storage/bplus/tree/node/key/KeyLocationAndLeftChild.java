package com.keks.kv_storage.bplus.tree.node.key;


import java.util.Objects;


public class KeyLocationAndLeftChild {

    public final KeyLocation keyLocation;
    public final long leftChild;

    public KeyLocationAndLeftChild(KeyLocation keyLocation, long leftChild) {
        this.keyLocation = keyLocation;
        this.leftChild = leftChild;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        KeyLocationAndLeftChild that = (KeyLocationAndLeftChild) o;
        return leftChild == that.leftChild && Objects.equals(keyLocation, that.keyLocation);
    }

    @Override
    public int hashCode() {
        return Objects.hash(keyLocation, leftChild);
    }
}
