package com.keks.kv_storage.server;

import com.keks.kv_storage.ex.KVServerException;


public abstract class KVServer {

    public abstract void start() throws KVServerException;
    public abstract void stop() throws KVServerException;

}
