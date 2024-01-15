package com.keks.kv_storage.server.http;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.keks.kv_storage.record.KVRecord;
import com.keks.kv_storage.KVStore;
import com.keks.kv_storage.io.InputStreamBufferedReader;
import com.keks.kv_storage.query.Query;
import com.keks.kv_storage.query.QueryIterator;
import com.keks.kv_storage.server.KVStorageCommand;
import com.keks.kv_storage.server.ServerCommandHandler;
import com.sun.net.httpserver.HttpExchange;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.rmi.RemoteException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import static com.keks.kv_storage.conf.ConfigParams.*;
import static com.keks.kv_storage.server.KVStorageCommand.GET_RECORDS;


public class HttpCommandHandler extends ServerCommandHandler<URI, HttpExchange, IOException> {

    private static final byte[] emptyByteArray = new byte[0];

    public HttpCommandHandler(KVStore kvStore) {
        super(
                kvStore,
                uri -> KVStorageCommand.valueOf(uri.getPath().split("/")[2].toUpperCase()), // first is /api1);
                HttpCommandHandler::getProperties);
    }

    // URI example: /put_entity?table=test2&key=3&value=10
    @Override
    public HttpExchange putEntity(URI in, HttpExchange out) throws IOException {
        Map<String, String> params = getParams(in);
        String dbName = getRequiredParamOrThrowException(params, DATABASE_NAME);
        String tableName = getRequiredParamOrThrowException(params, TABLE_NAME);
        String key = getRequiredParamOrThrowException(params, KEY_NAME);
        String requestMethod = out.getRequestMethod();
        if (requestMethod.equals("GET")) {
            String value = getRequiredParamOrThrowException(params, VALUE_DATA);
            kvStore.put(dbName, tableName, key, value.getBytes());
        } else {
            byte[] bytes = out.getRequestBody().readAllBytes();
            kvStore.put(dbName, tableName, key, bytes);
        }
        sendResponse(out, emptyByteArray, 200);
        return out;
    }

    // URI example: /put_batch_of_entities?table=test2&key=3&value=10

    @Override
    public HttpExchange putBatchOfEntities(String dbName, String tableName, Properties properties, HttpExchange out) throws IOException {
        if (!out.getRequestMethod().equals("POST"))
            throw new RemoteException("Method: " + GET_RECORDS + " should be post");

        InputStream is = out.getRequestBody();
        InputStreamBufferedReader bufferedReader = new InputStreamBufferedReader(is, 1024, 4);
        while (bufferedReader.hasNext()) {
            KVRecord kvRecord = bufferedReader.next();
            kvStore.put(dbName, tableName, kvRecord);
        }

        sendResponse(out, emptyByteArray, 200);
        return out;
    }



    // URI example: /get_entity?table=test2&key=3
    @Override
    public HttpExchange removeEntity(URI in, HttpExchange out) {
        Map<String, String> params = getParams(in);
        String dbName = getRequiredParamOrThrowException(params, DATABASE_NAME);
        String tableName = getRequiredParamOrThrowException(params, TABLE_NAME);
        String key = getRequiredParamOrThrowException(params, KEY_NAME);
        kvStore.remove(dbName, tableName, key);
        sendResponse(out, emptyByteArray, 200);
        return out;
    }

    @Override
    public HttpExchange getEntity(URI in, HttpExchange out) {
        Map<String, String> params = getParams(in);
        String dbName = getRequiredParamOrThrowException(params, DATABASE_NAME);
        String tableName = getRequiredParamOrThrowException(params, TABLE_NAME);
        String key = getRequiredParamOrThrowException(params, KEY_NAME);
        byte[] value = Optional.ofNullable(kvStore.get(dbName, tableName, key)).orElse(emptyByteArray);
        sendResponse(out, value, 200);
        return out;
    }

    @Override
    public HttpExchange getRecords(String dbName, String tableName, Properties properties, HttpExchange out) throws IOException {
        if (!out.getRequestMethod().equals("POST"))
            throw new RemoteException("Method: " + GET_RECORDS + " should be post");
        String str = new String(out.getRequestBody().readAllBytes());
        JsonNode jsonNode = new ObjectMapper().readTree(str);

        Query query = Query.fromJson(jsonNode);

        try (QueryIterator records = kvStore.getRecords(dbName, tableName, query);
             OutputStream outputStream = out.getResponseBody()) {

            out.sendResponseHeaders(200, 0);
            while (records.hasNext()) {
                KVRecord next = records.next();
                outputStream.write(next.getBytes());
            }
            outputStream.flush();

        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            System.out.println("Server closed");
        }
//
//        try (OutputStream outputStream = out.getResponseBody()) {
//            out.sendResponseHeaders(200, 0);
//            for (int i = 0; i < 1; i++) {
//                outputStream.write(new KVRecord("key" + String.format("%06d", i), "value" + String.format("%06d", i)).getBytes());
//
//            }
//            outputStream.flush();
//        } catch (Throwable e) {
//            e.printStackTrace();
//        }
        return out;
    }

    @Override
    public HttpExchange getRecordsCnt(String dbName, String tableName, Properties properties, HttpExchange out) throws IOException {
        if (!out.getRequestMethod().equals("POST"))
            throw new RemoteException("Method: " + GET_RECORDS + " should be post");
        String str = new String(out.getRequestBody().readAllBytes());
        JsonNode jsonNode = new ObjectMapper().readTree(str);

        Query query = Query.fromJson(jsonNode);

        try (OutputStream outputStream = out.getResponseBody()) {
            String recordsCnt = String.valueOf(kvStore.getRecordsCnt(dbName, tableName, query));
            out.sendResponseHeaders(200, recordsCnt.length());
            outputStream.write(recordsCnt.getBytes());
            outputStream.flush();
        }
        return out;
    }

    @Override
    public HttpExchange createDB(String dbName, Properties properties, HttpExchange out) throws IOException {
        kvStore.createDB(dbName);
        sendResponse(out, emptyByteArray, 200);
        return out;
    }

    @Override
    public HttpExchange dropDB(String dbName, Properties properties, HttpExchange out) throws IOException {
        kvStore.dropDB(dbName);
        sendResponse(out, emptyByteArray, 200);
        return out;
    }

    // URI example: /create_table?table=test3&sparseIndexSize=3&memCacheSize=10&bloomFilterFalsePositiveRate=0.1
    @Override
    public HttpExchange createTable(String dbName, String tableName, String engineName, Properties properties, HttpExchange out) {
        kvStore.createTable(dbName, tableName, engineName, properties);
        sendResponse(out, emptyByteArray, 200);
        return out;
    }

    // URI example: /delete_table?table=test2
    @Override
    public HttpExchange dropTable(String dbName, String tableName, Properties properties, HttpExchange out) {
        kvStore.dropTable(dbName, tableName);
        sendResponse(out, emptyByteArray, 200);
        return out;
    }

    // URI example: /full_compact_table?table=test2
    @Override
    public HttpExchange optimizeTable(String dbName, String tableName, Properties properties, HttpExchange out) {
        kvStore.optimizeTable(dbName, tableName);
        sendResponse(out, emptyByteArray, 200);
        return out;
    }

    // URI example: /flush_table?table=test2
    @Override
    public HttpExchange flushTable(String dbName, String tableName, Properties properties, HttpExchange out) {
        kvStore.flushTable(dbName, tableName);
        sendResponse(out, emptyByteArray, 200);
        return out;
    }

    @Override
    public HttpExchange makeCheckpoint(String dbName, String tableName, Properties properties, HttpExchange out) {
        kvStore.makeCheckpoint(dbName, tableName);
        sendResponse(out, emptyByteArray, 200);
        return out;
    }

    @Override
    public HttpExchange getDatabases(Properties properties, HttpExchange out) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        ArrayNode arrayNode = mapper.createArrayNode();
        kvStore.getDatabasesList().forEach(arrayNode::add);
        sendResponse(out, arrayNode.toPrettyString().getBytes(), 200);
        return out;
    }

    @Override
    public HttpExchange getTables(String dbName, Properties properties, HttpExchange out) {
        ObjectMapper mapper = new ObjectMapper();
        ArrayNode arrayNode = mapper.createArrayNode();
        kvStore.getTablesList(dbName).forEach(arrayNode::add);
        sendResponse(out, arrayNode.toPrettyString().getBytes(), 200);
        return out;
    }

    @Override
    public HttpExchange getTableParameters(String dbName, String tableName, Properties properties, HttpExchange out) {
        JsonNode engineJson = kvStore.getEngineParameters(dbName, tableName).getAsJson();
        JsonNode tableJson = kvStore.getKvTableParameters(dbName, tableName).getAsJson();
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode objectNode = mapper.createObjectNode();
        objectNode.set("engine", engineJson);
        objectNode.set("table", tableJson);
        sendResponse(out, objectNode.toPrettyString().getBytes(), 200);
        return out;
    }

    private static Map<String, String> getParams(URI url) {
        Map<String, String> query_pairs = new HashMap<>();
        String query = url.getQuery();
        String[] pairs = query.split("&");
        for (String pair : pairs) {
            String[] split = pair.split("=", 2);
            query_pairs.put(split[0].toUpperCase(), split[1]);
        }
        return query_pairs;
    }

    private static Properties getProperties(URI url) {
        Properties properties = new Properties();
        String query = url.getQuery();
        if (query != null) {
            String[] pairs = query.split("&");
            for (String pair : pairs) {
                String[] split = pair.split("=", 2);
                properties.put(split[0], split[1]);
            }
        }
        return properties;
    }

    private static void sendResponse(HttpExchange httpExchange, byte[] value, int statusCode) {
        try (OutputStream outputStream = httpExchange.getResponseBody()) {
            httpExchange.sendResponseHeaders(statusCode, value.length);
            outputStream.write(value);
            outputStream.flush();
        } catch (Throwable e) {
            e.printStackTrace();
        }
    }

}
