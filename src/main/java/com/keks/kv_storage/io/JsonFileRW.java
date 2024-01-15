package com.keks.kv_storage.io;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.IOException;


public class JsonFileRW {

    public static void write(File file, JsonNode jsonNode) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        mapper.writeValue(file, jsonNode);
    }

    public static JsonNode read(File file) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readTree(file);
    }

}
