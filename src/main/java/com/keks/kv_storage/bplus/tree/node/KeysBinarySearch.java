package com.keks.kv_storage.bplus.tree.node;


import com.keks.kv_storage.bplus.page_manager.page_disk.sp.SlotLocation;


public abstract class KeysBinarySearch<T extends SlotLocation> {

    public abstract int getNextKeyNum();

    public abstract T getKeyLocation(int keyNum);

}
