package com.keks.kv_storage.conf;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;


public abstract class Params<T extends ParamParser> {

    protected final HashMap<String, Object> conf;

    public Params(T[] enums, Properties properties) {
        HashMap<String, Object> confMap = new HashMap<>();
        for (T param : enums) {
            Object propertyValue = properties.get(param);
            if (propertyValue == null) {
                propertyValue = properties.get(param.getName());
            }
            if (propertyValue == null) {
                propertyValue = properties.getProperty(param.getName());
            }
            if (propertyValue == null) {
                confMap.put(param.getName(), param.getDefaultValue());
            } else {
                confMap.put(param.getName(), param.parse(propertyValue));
            }
        }
        this.conf = confMap;
    }

    public Params(T[] enums, JsonNode rootNode) {
        HashMap<String, Object> confMap = new HashMap<>();
        for (T param : enums) {
            String jsonValue = rootNode.get(param.getName()).textValue();
            if (jsonValue == null) {
                confMap.put(param.getName(), param.getDefaultValue());
            } else {
                confMap.put(param.getName(), param.parse(jsonValue));
            }
        }
        this.conf = confMap;
    }

    public JsonNode getAsJson() {
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode rootNode = mapper.createObjectNode();
        for (Map.Entry<String, Object> entry : conf.entrySet()) {
            rootNode.put(entry.getKey(), entry.getValue().toString());
        }
        return rootNode;
    }

    public HashMap<String, Object> getAsMap() {
        return new HashMap<>(conf);
    }

    protected <R> R getConfParam(T param) {
        return (R) conf.get(param.getName());
    }

}
