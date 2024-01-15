package com.keks.kv_storage.io;

import com.fasterxml.jackson.databind.ObjectMapper;


public abstract class JsonFile implements AutoCloseable {

    public final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void close() {}

}
