package com.keks.kv_storage.client;

import com.keks.kv_storage.record.KVRecord;
import com.keks.kv_storage.conf.TableEngineType;
import com.keks.kv_storage.ex.TableNotFoundException;
import com.keks.kv_storage.query.Query;
import com.keks.kv_storage.query.range.RangeKey;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;


public class KvDslClient {

    private final KVServerHttpClient kvServerHttpClient;

    public KvDslClient(String serverHost, int serverPort) {
        this.kvServerHttpClient = new KVServerHttpClient(serverHost, serverPort);
    }

    public void createDb(String dbName) throws URISyntaxException, IOException, InterruptedException {
        kvServerHttpClient.sendCreateDBRequest(dbName);
    }

    public void createTable(String dbName,
                            String tblName,
                            TableEngineType engine,
                            HashMap<String, Object> params) throws URISyntaxException, IOException, InterruptedException {
        kvServerHttpClient.sendCreateTableRequest(dbName, tblName, engine, params);
    }


    public HashSet<String> getDbs() throws URISyntaxException, IOException, InterruptedException {
        return kvServerHttpClient.sendGetDBsRequest();
    }

    public HashSet<String> getTables(String dbName) throws URISyntaxException, IOException, InterruptedException {
        return kvServerHttpClient.sendGetTablesRequest(dbName);
    }

    public TableReadActions readTable(String dbName, String tblName) throws URISyntaxException, IOException, InterruptedException {
        String dbNameLower = dbName.toLowerCase();
        String tblNameLower = tblName.toLowerCase();
        HashSet<String> tbls = getTables(dbName);
        if (!tbls.contains(tblName.toUpperCase())) throw new TableNotFoundException(dbNameLower, tblNameLower);
        return new TableReadActions(dbName, tblName);
    }

    public TableWriteActions writeTable(String dbName, String tblName) throws URISyntaxException, IOException, InterruptedException {
        String dbNameLower = dbName.toLowerCase();
        String tblNameLower = tblName.toLowerCase();
        HashSet<String> tbls = getTables(dbName);
        if (!tbls.contains(tblName.toUpperCase())) throw new TableNotFoundException(dbNameLower, tblNameLower);
        return new TableWriteActions(dbName, tblName);
    }

    public class TableReadActions {

        private final String tableName;
        private final String dbName;
        private final Query.QueryBuilder queryBuilder;

        public TableReadActions(String dbName, String tableName) {
            this.dbName = dbName;
            this.tableName = tableName;
            this.queryBuilder = new Query.QueryBuilder();
        }

        public TableReadActions(TableReadActions prevActions, Query.QueryBuilder queryBuilder) {
            this.tableName = prevActions.tableName;
            this.dbName = prevActions.dbName;
            this.queryBuilder = queryBuilder;
        }

        public TableReadActions withNoMinBound() {
            return new TableReadActions(this, queryBuilder.withNoMinBound());
        }

        public TableReadActions withMinKey(RangeKey key) {
            return new TableReadActions(this, queryBuilder.withMinKey(key));
        }

        public TableReadActions withMinKey(String key, boolean inclusive) {
            return new TableReadActions(this, queryBuilder.withMinKey(key, inclusive));
        }

        public TableReadActions withNoMaxBound() {
            return new TableReadActions(this, queryBuilder.withNoMaxBound());
        }

        public TableReadActions withMaxKey(RangeKey key) {
            return new TableReadActions(this, queryBuilder.withMaxKey(key));
        }

        public TableReadActions withMaxKey(String key, boolean inclusive) {
            return new TableReadActions(this, queryBuilder.withMaxKey(key, inclusive));
        }

        public TableReadActions withNoLimit() {
            return new TableReadActions(this, queryBuilder.withNoLimit());
        }

        public TableReadActions withLimit(long limit) {
            return new TableReadActions(this, queryBuilder.withLimit(limit));
        }

        public long count() throws URISyntaxException, IOException, InterruptedException {
            return kvServerHttpClient.getRecordsCnt(dbName, tableName, queryBuilder.build());
        }

        public Iterator<KVRecord> getRecordsIter() throws URISyntaxException, IOException, InterruptedException {
            return kvServerHttpClient.getRange(dbName, tableName, 1024, queryBuilder.build());
        }

        public ArrayList<KVRecord> getRecordsCollect() throws URISyntaxException, IOException, InterruptedException {
            Iterator<KVRecord> recordsIter = getRecordsIter();
            ArrayList<KVRecord> res = new ArrayList<>(100); // TODO add aprox count to set initial
            recordsIter.forEachRemaining(res::add);
            return res;
        }



    }

     public class TableWriteActions {
        private final String tableName;
        private final String dbName;

        public TableWriteActions(String dbName, String tableName) {
            this.dbName = dbName;
            this.tableName = tableName;
        }

        public void write(Iterable<KVRecord> records) throws URISyntaxException, IOException, InterruptedException {
            kvServerHttpClient.sendPutBatchOfEntitiesRequest(dbName, tableName, records);
        }

    }


}
