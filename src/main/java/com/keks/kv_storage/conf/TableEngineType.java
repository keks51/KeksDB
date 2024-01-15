package com.keks.kv_storage.conf;


public enum TableEngineType {
    LSM,
    BPLUS;

    @Override
    public String toString() {
        return super.toString().toLowerCase();
    }
}
