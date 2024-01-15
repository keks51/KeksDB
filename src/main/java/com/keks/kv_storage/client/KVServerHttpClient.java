package com.keks.kv_storage.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.keks.kv_storage.record.KVRecord;
import com.keks.kv_storage.conf.ConfigParams;
import com.keks.kv_storage.conf.TableEngineType;
import com.keks.kv_storage.io.InputStreamBufferedReader;
import com.keks.kv_storage.query.Query;
import com.keks.kv_storage.server.KVStorageCommand;
import com.keks.kv_storage.server.http.UriBuilder;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;


public class KVServerHttpClient {

    private static final HttpResponse.BodyHandler<String> emptyBStringBodyHandler = (responseInfo) -> HttpResponse.BodySubscribers.mapping(
            HttpResponse.BodySubscribers.ofString(Charset.defaultCharset()),
            e -> e);

    private static final HttpResponse.BodyHandler<byte[]> emptyBytesBodyHandler = (responseInfo) -> HttpResponse.BodySubscribers.mapping(
            HttpResponse.BodySubscribers.ofByteArray(),
            e -> (e.length == 0) ? null : e);

    private static final HttpResponse.BodyHandler<InputStream> inputStreamBodyHandler = (responseInfo) -> HttpResponse.BodySubscribers.mapping(
            HttpResponse.BodySubscribers.ofInputStream(),
            e -> (e));

    private final String serverHost;
    private final int serverPort;
    public final HttpClient httpClient;
    public final int version;

    public final UriBuilder uriBuilder;

    public KVServerHttpClient(String serverHost,
                              int serverPort) {
        this(serverHost, serverPort, 1);
    }

    public KVServerHttpClient(String serverHost,
                              int serverPort, int version) {
        this.serverHost = serverHost;
        this.serverPort = serverPort;
        this.httpClient = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build();
        this.version = version;
        this.uriBuilder = new UriBuilder("http", serverHost, serverPort);
    }

    // (GET http://localhost:8866/api1/create_db?database_name=<db_name>)
    public HttpResponse<String> sendCreateDBRequest(String databaseName) throws URISyntaxException, IOException, InterruptedException {
        URI uri = uriBuilder.copy()
                .setPathSegments("api1", KVStorageCommand.CREATE_DB.toString())
                .addParameter(ConfigParams.DATABASE_NAME, databaseName)
                .build();
        return sendGetRequest(uri);
    }

    // (GET http://localhost:8866/api1/drop_db?database_name=<db_name>)
    public HttpResponse<String> sendDropDBRequest(String databaseName) throws URISyntaxException, IOException, InterruptedException {
        URI uri = uriBuilder.copy()
                .setPathSegments("api1", KVStorageCommand.DROP_DB.toString())
                .addParameter(ConfigParams.DATABASE_NAME, databaseName)
                .build();
        return sendGetRequest(uri);
    }

    // (GET http://localhost:8866/api1/create_table?database_name=<db_name>&table_name=<table_name>&engine_name=<engine>&lsm_SPARSE_INDEX_SIZE=<>&lsm_max_number_of_records_in_memory=<>&lsm_bloom_filter_false_positive_rate=<>) 200
    public HttpResponse<String> sendCreateTableRequest(String db,
                                                       String table,
                                                       TableEngineType engine,
                                                       HashMap<String, Object> params) throws URISyntaxException, IOException, InterruptedException {
        URI uri = uriBuilder.copy()
                .setPathSegments("api1", KVStorageCommand.CREATE_TABLE.toString())
                .addParameter(ConfigParams.DATABASE_NAME, db)
                .addParameter(ConfigParams.TABLE_NAME, table)
                .addParameter(ConfigParams.ENGINE_NAME, engine.name().toLowerCase())
                .addParameters(params)
                .build();
        return sendGetRequest(uri);
    }

    // (GET http://localhost:8866/api1/drop_table?database_name=<>&table_name=<>) 200
    public HttpResponse<String> sendDropTableRequest(String db,
                                                     String table) throws URISyntaxException, IOException, InterruptedException {
        URI uri = uriBuilder.copy()
                .setPathSegments("api1", KVStorageCommand.DROP_TABLE.toString())
                .addParameter(ConfigParams.DATABASE_NAME, db)
                .addParameter(ConfigParams.TABLE_NAME, table)
                .build();
        return sendGetRequest(uri);
    }

    // TODO test without sync
    // (GET http://localhost:8866/api1/put_entity?database_name=<>&table_name=<>&key_name=<>&value_data=<>)
    synchronized public HttpResponse<String> sendPutEntityRequest(String db,
                                                                  String table,
                                                                  String key,
                                                                  String value) throws URISyntaxException, IOException, InterruptedException {
        URI uri = uriBuilder.copy()
                .setPathSegments("api1", KVStorageCommand.PUT_ENTITY.toString())
                .addParameter(ConfigParams.DATABASE_NAME, db)
                .addParameter(ConfigParams.TABLE_NAME, table)
                .addParameter(ConfigParams.KEY_NAME, key)
                .addParameter(ConfigParams.VALUE_DATA, value)
                .build();
        return sendGetRequest(uri);
    }

    synchronized public HttpResponse<String> sendPutEntityRequest(String db,
                                                                  String table,
                                                                  String key,
                                                                  byte[] value) throws URISyntaxException, IOException, InterruptedException {
        URI uri = uriBuilder.copy()
                .setPathSegments("api1", KVStorageCommand.PUT_ENTITY.toString())
                .addParameter(ConfigParams.DATABASE_NAME, db)
                .addParameter(ConfigParams.TABLE_NAME, table)
                .addParameter(ConfigParams.KEY_NAME, key)
                .build();
        return sendPostRequest(uri, value);
    }

    synchronized public HttpResponse<String> sendPutBatchOfEntitiesRequest(String db,
                                                                           String table,
                                                                           Iterable<KVRecord> records) throws URISyntaxException, IOException, InterruptedException {
        URI uri = uriBuilder.copy()
                .setPathSegments("api1", KVStorageCommand.PUT_BATCH_OF_ENTITIES.toString())
                .addParameter(ConfigParams.DATABASE_NAME, db)
                .addParameter(ConfigParams.TABLE_NAME, table)
                .build();

        Iterable<byte[]> byteIterator2 = () -> new Iterator<>() {

            private Iterator<KVRecord> iter = records.iterator();

            @Override
            public boolean hasNext() {
                return iter.hasNext();
            }

            @Override
            public byte[] next() {
                return iter.next().getBytes();
            }
        };


        HttpRequest request2 = HttpRequest.newBuilder()
                .uri(uri)
                .POST(HttpRequest.BodyPublishers.ofByteArrays(byteIterator2))
                .build();

        HttpResponse<String> res = httpClient.send(request2, emptyBStringBodyHandler);
        return res;
    }

    // (GET http://localhost:8866/api1/remove_entity?database_name=<>&table_name=<>&key_name=<>)
    synchronized public HttpResponse<String> sendRemoveEntityRequest(String db,
                                                                     String table,
                                                                     String key) throws URISyntaxException, IOException, InterruptedException {
        URI uri = uriBuilder.copy()
                .setPathSegments("api1", KVStorageCommand.REMOVE_ENTITY.toString())
                .addParameter(ConfigParams.DATABASE_NAME, db)
                .addParameter(ConfigParams.TABLE_NAME, table)
                .addParameter(ConfigParams.KEY_NAME, key)
                .build();
        return sendGetRequest(uri);
    }


    // (GET http://localhost:8866/api1/get_entity?database_name=<>&table_name=<>&key_name=<>)
    public HttpResponse<byte[]> sendGetEntityRequest(String db,
                                                     String table,
                                                     String key) throws URISyntaxException, IOException, InterruptedException {
        URI uri = uriBuilder.copy()
                .setPathSegments("api1", KVStorageCommand.GET_ENTITY.toString())
                .addParameter(ConfigParams.DATABASE_NAME, db)
                .addParameter(ConfigParams.TABLE_NAME, table)
                .addParameter(ConfigParams.KEY_NAME, key)
                .build();

        return this.sendGetRequest(uri, emptyBytesBodyHandler);
    }

    // (GET http://localhost:8866/api1/get_records_cnt?database_name=<>&table_name)
    public int getRecordsCnt(String db,
                             String table,
                             Query query) throws URISyntaxException, IOException, InterruptedException {
        URI uri = uriBuilder.copy()
                .setPathSegments("api1", KVStorageCommand.GET_RECORDS_CNT.toString())
                .addParameter(ConfigParams.DATABASE_NAME, db)
                .addParameter(ConfigParams.TABLE_NAME, table)
                .build();

        HttpRequest request = HttpRequest.newBuilder()
                .uri(uri)
                .POST(HttpRequest.BodyPublishers.ofString(query.toJson().toPrettyString()))
//                .header("Connection", "Keep-Alive") // TODO test what it is
//                .header("Keep-Alive", "timeout=50000, max=1") // TODO test what it is
//                .timeout(Duration.ofSeconds(1000)) // TODO test what it is
                .build();
        HttpResponse<String> res = httpClient.send(request, emptyBStringBodyHandler);
        return Integer.parseInt(res.body());
    }

    // (GET http://localhost:8866/api1/get_all?database_name=<>&table_name=<>)
    public Iterator<KVRecord> getAll(String db,
                                     String table,
                                     int bufSize) throws URISyntaxException, IOException, InterruptedException {
        return getRange(db, table, bufSize, new Query.QueryBuilder().build());
    }

    public Iterator<KVRecord> getRange(String db,
                                       String table,
                                       int bufSize,
                                       Query query) throws URISyntaxException, IOException, InterruptedException {
        assert bufSize % 1024 == 0;
        URI uri = uriBuilder.copy()
                .setPathSegments("api1", KVStorageCommand.GET_RECORDS.toString())
                .addParameter(ConfigParams.DATABASE_NAME, db)
                .addParameter(ConfigParams.TABLE_NAME, table)
                .build();
        HttpRequest request = HttpRequest.newBuilder()
                .uri(uri)
                .POST(HttpRequest.BodyPublishers.ofString(query.toJson().toPrettyString()))
//                .header("Connection", "Keep-Alive") // TODO test what it is
//                .header("Keep-Alive", "timeout=50000, max=1") // TODO test what it is
//                .timeout(Duration.ofSeconds(1000)) // TODO test what it is
                .build();

        HttpResponse<InputStream> send = httpClient.send(request, inputStreamBodyHandler);

        return new Iterator<>() {
            private final InputStream is = send.body();
            private final InputStreamBufferedReader bufferedReader = new InputStreamBufferedReader(is, bufSize, 4);

            @Override
            public boolean hasNext() {
                boolean hasNext = bufferedReader.hasNext();
                if (!hasNext) {
                    try {
                        is.close();
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
                return hasNext;
            }

            @Override
            public KVRecord next() {
                KVRecord next = bufferedReader.next();
                return next;
            }

        };
    }

    // (GET http://localhost:8866/api1/optimize_table?database_name=<>&table_name=<>)
    public HttpResponse<String> sendOptimizeTableRequest(String db,
                                                         String table) throws URISyntaxException, IOException, InterruptedException {
        URI uri = uriBuilder.copy()
                .setPathSegments("api1", KVStorageCommand.OPTIMIZE_TABLE.toString())
                .addParameter(ConfigParams.DATABASE_NAME, db)
                .addParameter(ConfigParams.TABLE_NAME, table)
                .build();
        return sendGetRequest(uri);
    }

    // (GET http://localhost:8866/api1/flush_table?database_name=<>&table_name=<>)
    public HttpResponse<String> sendFlushTableRequest(String db,
                                                      String table) throws URISyntaxException, IOException, InterruptedException {
        URI uri = uriBuilder.copy()
                .setPathSegments("api1", KVStorageCommand.FLUSH_TABLE.toString())
                .addParameter(ConfigParams.DATABASE_NAME, db)
                .addParameter(ConfigParams.TABLE_NAME, table)
                .build();
        return sendGetRequest(uri);
    }

    // (GET http://localhost:8866/api1/flush_table?database_name=<>&table_name=<>)
    public HttpResponse<String> sendMakeTableCheckpointRequest(String db,
                                                               String table) throws URISyntaxException, IOException, InterruptedException {
        URI uri = uriBuilder.copy()
                .setPathSegments("api1", KVStorageCommand.MAKE_CHECKPOINT.toString())
                .addParameter(ConfigParams.DATABASE_NAME, db)
                .addParameter(ConfigParams.TABLE_NAME, table)
                .build();
        return sendGetRequest(uri);
    }

    // (GET http://localhost:8866/api1/get_databases)
    public HashSet<String> sendGetDBsRequest() throws URISyntaxException, IOException, InterruptedException {
        URI uri = uriBuilder.copy()
                .setPathSegments("api1", KVStorageCommand.GET_DATABASES.toString())
                .build();
        HttpResponse<String> httpResponse = sendGetRequest(uri);
        if (httpResponse.statusCode() != 200) {
            throw new RuntimeException("Response is not 200\n" + httpResponse);
        }
        ObjectMapper objectMapper = new ObjectMapper();
        String[] strings = objectMapper.readValue(httpResponse.body(), String[].class);
        HashSet<String> dbs = new HashSet<>(Arrays.asList(strings));
        return dbs;
    }

    public HashSet<String> sendGetTablesRequest(String db) throws URISyntaxException, IOException, InterruptedException {
        URI uri = uriBuilder.copy()
                .setPathSegments("api1", KVStorageCommand.GET_TABLES.toString())
                .addParameter(ConfigParams.DATABASE_NAME, db)
                .build();

        HttpResponse<String> httpResponse = sendGetRequest(uri);
        if (httpResponse.statusCode() != 200) {
            throw new RuntimeException("Response is not 200\n" + httpResponse);
        }
        ObjectMapper objectMapper = new ObjectMapper();
        String[] strings = objectMapper.readValue(httpResponse.body(), String[].class);
        HashSet<String> tbls = new HashSet<>(Arrays.asList(strings));
        return tbls;
    }

    public HttpResponse<String> sendGetTableParameters(String db,
                                                       String table) throws URISyntaxException, IOException, InterruptedException {
        URI uri = uriBuilder.copy()
                .setPathSegments("api1", KVStorageCommand.GET_TABLE_PARAMETERS.toString())
                .addParameter(ConfigParams.DATABASE_NAME, db)
                .addParameter(ConfigParams.TABLE_NAME, table)
                .build();
        return sendGetRequest(uri);
    }

    // localhost:8765/get_table_statistics?table=test2
    public HttpResponse<String> sendGetTableStatisticsRequest(String db,
                                                              String table) throws URISyntaxException, IOException, InterruptedException {
        return sendGetRequest(new URI("http://" + serverHost + ":" + serverPort + "/get_table_statistics?" + "table=" + table));
    }

    private HttpResponse<String> sendGetRequest(URI uri) throws IOException, InterruptedException {
        return sendGetRequest(uri, HttpResponse.BodyHandlers.ofString());
    }


    private <T> HttpResponse<T> sendGetRequest(URI uri, HttpResponse.BodyHandler<T> responseBodyHandler) throws IOException, InterruptedException {
        HttpRequest request2 = HttpRequest.newBuilder()
                .uri(uri)
                .GET()
//                .header("Connection", "Keep-Alive")
//                .header("Keep-Alive", "timeout=50000, max=1")
//                .timeout(Duration.ofSeconds(1000))
                .build();
//

        HttpResponse<T> send = httpClient.send(request2, responseBodyHandler);

        return send;
    }

    private HttpResponse<String> sendPostRequest(URI uri, byte[] body) throws IOException, InterruptedException {
        HttpRequest request2 = HttpRequest.newBuilder()
                .uri(uri)
                .POST(HttpRequest.BodyPublishers.ofString(new String(body)))
                .build();

        return httpClient.send(request2, HttpResponse.BodyHandlers.ofString());
    }

//    private void sendAsyncRequest(URI uri) throws IOException, InterruptedException {
//        HttpRequest request2 = HttpRequest.newBuilder()
//                .uri(uri)
//                .GET()
//                .header("Connection", "close")
//                .timeout(Duration.ofSeconds(10))
//                .build();
//        CompletableFuture<HttpResponse<String>> httpResponseCompletableFuture = httpClient.sendAsync(request2, HttpResponse.BodyHandlers.ofString());
//        httpResponseCompletableFuture.cancel(true);
//    }


//    public void sendPutEntityAsyncRequest(String table, String key, String value) throws URISyntaxException, IOException, InterruptedException {
//        sendAsyncRequest(new URI("http://" + serverHost + ":" + serverPort + "/put_entity?" + "table=" + table + "&key=" + key + "&value=" + value));
//    }

}
