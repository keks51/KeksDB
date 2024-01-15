package com.keks.kv_storage.server;

public enum KVStorageCommand {
    CREATE_DB,
    DROP_DB,
    CREATE_TABLE,
    DROP_TABLE,
    PUT_ENTITY,
    PUT_BATCH_OF_ENTITIES,
    REMOVE_ENTITY,
    GET_ENTITY,
    GET_RECORDS_CNT,
    GET_RECORDS,
    OPTIMIZE_TABLE,
    FLUSH_TABLE,
    MAKE_CHECKPOINT,
    GET_DATABASES,
    GET_TABLES,
    GET_TABLE_PARAMETERS;

    @Override
    public String toString() {
        return name().toLowerCase();
    }

}
