package com.keks.kv_storage.server;

import com.keks.kv_storage.KVStore;
import com.keks.kv_storage.utils.UnCheckedFunction;

import java.util.Map;
import java.util.Properties;

import static com.keks.kv_storage.conf.ConfigParams.*;


public abstract class ServerCommandHandler<IN, OUT, EX extends Exception> {

    public final KVStore kvStore;

    private final UnCheckedFunction<IN, KVStorageCommand, EX> commandParser;
    private final UnCheckedFunction<IN, Properties, EX> propertiesParser;


    public ServerCommandHandler(KVStore kvStore,
                                UnCheckedFunction<IN, KVStorageCommand, EX> commandParser,
                                UnCheckedFunction<IN, Properties, EX> propertiesParser) {
        this.kvStore = kvStore;
        this.commandParser = commandParser;
        this.propertiesParser = propertiesParser;
    }

    public OUT executeCommand(IN in, OUT out) throws EX {
        KVStorageCommand command = commandParser.apply(in);

        // for fast parsing
        switch (command) {
            case PUT_ENTITY:
                return putEntity(in, out);
            case REMOVE_ENTITY:
                return removeEntity(in, out);
            case GET_ENTITY:
                return getEntity(in, out);

        }
        Properties commandProperties = propertiesParser.apply(in);

        switch (command) {
            case GET_DATABASES:
                return getDatabases(commandProperties, out);
        }


        String dbName = getRequiredParamOrThrowException(commandProperties, DATABASE_NAME);
        switch (command) {
            case CREATE_DB:
                return createDB(dbName, commandProperties, out);
            case DROP_DB:
                return dropDB(dbName, commandProperties, out);
            case GET_TABLES:
                return getTables(dbName, commandProperties, out);
        }

        String tableName = getRequiredParamOrThrowException(commandProperties, TABLE_NAME);
        switch (command) {
            case PUT_BATCH_OF_ENTITIES:
                return putBatchOfEntities(dbName, tableName, commandProperties, out);
            case GET_RECORDS:
                return getRecords(dbName, tableName, commandProperties, out);
            case GET_RECORDS_CNT:
                return getRecordsCnt(dbName, tableName, commandProperties, out);
            case CREATE_TABLE:
                String engineName = getRequiredParamOrThrowException(commandProperties, ENGINE_NAME);
                return createTable(dbName, tableName, engineName, commandProperties, out);
            case DROP_TABLE:
                return dropTable(dbName, tableName, commandProperties, out);
            case OPTIMIZE_TABLE:
                return optimizeTable(dbName, tableName, commandProperties, out);
            case FLUSH_TABLE:
                return flushTable(dbName, tableName, commandProperties, out);
            case MAKE_CHECKPOINT:
                return makeCheckpoint(dbName, tableName, commandProperties, out);
            case GET_TABLE_PARAMETERS:
                return getTableParameters(dbName, tableName, commandProperties, out);
            default:
                throw new RuntimeException("Not implemented action: " + command);
        }

    }

    // URI example: /put_entity?database_name=test_db&table_name=test2&key=3&value=10
    public abstract OUT putEntity(IN in, OUT out) throws EX;

    // URI example: /get_entity?database_name=test_db&table_name=test2&key=3
    public abstract OUT getEntity(IN in, OUT out) throws EX;

    // URI example: /get_entity?database_name=test_db&table_name=test2
    public abstract OUT putBatchOfEntities(String dbName, String tableName, Properties properties, OUT out) throws EX;
    
    // URI example: /get_all?database_name=test_db&table_name=test2
    public abstract OUT getRecords(String dbName, String tableName, Properties properties, OUT out) throws EX;


    // URI example: /get_records_cnt?database_name=test_db&table_name=test2
    public abstract OUT getRecordsCnt(String dbName, String tableName, Properties properties, OUT out) throws EX;

    // URI example: /delete_entity?database_name=test_db&table_name=test2&key=3
    public abstract OUT removeEntity(IN in, OUT out) throws EX;


    public abstract OUT createDB(String dbName, Properties properties, OUT out) throws EX;

    public abstract OUT dropDB(String dbName, Properties properties, OUT out) throws EX;

    public abstract OUT createTable(String dbName, String tableName, String engineMane, Properties properties, OUT out) throws EX;

    // URI example: /delete_table?database_name=test_db&table_name=test2
    public abstract OUT dropTable(String dbName, String tableName, Properties properties, OUT out) throws EX;

    // URI example: /full_compact_table?database_name=test_db&table_name=test2
    public abstract OUT optimizeTable(String dbName, String tableName, Properties properties, OUT out) throws EX;

    // URI example: /flush_table?database_name=test_db&table_name=test2
    public abstract OUT flushTable(String dbName, String tableName, Properties properties, OUT out) throws EX;

    public abstract OUT makeCheckpoint(String dbName, String tableName, Properties properties, OUT out) throws EX;

    public abstract OUT getDatabases(Properties properties, OUT out) throws EX;

    public abstract OUT getTables(String dbNam, Properties properties, OUT out) throws EX;

    public abstract OUT getTableParameters(String dbName, String tableName, Properties properties, OUT out) throws EX;

    public static String getRequiredParamOrThrowException(Map<String, String> params, String param) {
        String value = params.get(param);
        if (value == null) {
            throw new RuntimeException("Parameter: '" + param + "' is required");
        }
        return value;
    }

    public static String getRequiredParamOrThrowException(Properties properties, String param) {
        String value = properties.getProperty(param);
        if (value == null) {
            throw new IllegalArgumentException("Parameter: '" + param + "' is required");
        }
        return value;
    }

}
