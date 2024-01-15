package com.keks.kv_storage.server.thrift;

import com.keks.kv_storage.KVStore;
import com.keks.kv_storage.ex.NotImplementedException;
import com.keks.kv_storage.server.KVStorageCommand;
import com.keks.kv_storage.server.ServerCommandHandler;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TType;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;


public class ThriftServerCommandHandler extends ServerCommandHandler<TProtocol, TProtocol, TException> {

    private static final ByteBuffer empty = ByteBuffer.allocateDirect(0);
    public ThriftServerCommandHandler(KVStore kvStore) {
        super(kvStore,
                tProtocol -> KVStorageCommand.valueOf(tProtocol.readString().toUpperCase()),
                tProtocol -> {
                    Properties properties = new Properties();

                    Map<String, String> map = tProtocol.readMap(
                            new TProtocol.ReadMapEntryCallback<>() {
                                @Override
                                public String getKey() throws TException {
                                    return tProtocol.readString();
                                }

                                @Override
                                public String getValue() throws TException {
                                    return tProtocol.readString();
                                }
                            },
                            HashMap::new
                    );
                    properties.putAll(map);
                    return properties;
                });

    }


    @Override
    public TProtocol putEntity(TProtocol in, TProtocol out) throws TException {
        String dbName = in.readString();
        String tableName = in.readString();
        String key = in.readString();
        ByteBuffer byteBuffer = in.readBinary();
        kvStore.put(dbName, tableName, key, byteBuffer.array());
        writeOkStatus(out);
        return out;
    }

    @Override
    public TProtocol getEntity(TProtocol in, TProtocol out) throws TException {
        String dbName = in.readString();
        String tableName = in.readString();
        String key = in.readString();
        byte[] value = kvStore.get(dbName, tableName, key);
        writeOkStatus(out);
        if (value == null) {
            out.writeBinary(empty);
        } else {
            out.writeBinary(ByteBuffer.wrap(value));
        }

        return out;
    }

    @Override
    public TProtocol putBatchOfEntities(String dbName, String tableName, Properties properties, TProtocol tProtocol) throws TException {
        throw new NotImplementedException();
    }

    @Override
    public TProtocol getRecords(String dbName, String tableName, Properties properties, TProtocol tProtocol2) throws TException {
        throw new NotImplementedException();
    }

    @Override
    public TProtocol getRecordsCnt(String dbName, String tableName, Properties properties, TProtocol tProtocol2) throws TException {
        throw new NotImplementedException();
    }

    @Override
    public TProtocol removeEntity(TProtocol in, TProtocol out) throws TException {
        String dbName = in.readString();
        String tableName = in.readString();
        String key = in.readString();
        kvStore.remove(dbName, tableName, key);
        writeOkStatus(out);
        return out;
    }


    @Override
    public TProtocol createDB(String dbName, Properties properties, TProtocol out) throws TException {
        kvStore.createDB(dbName);
        writeOkStatus(out);
        return out;
    }

    @Override
    public TProtocol dropDB(String dbName, Properties properties, TProtocol out) throws TException {
        kvStore.dropDB(dbName);
        writeOkStatus(out);
        return out;
    }

    @Override
    public TProtocol createTable(String dbName, String tableName, String engineName, Properties properties, TProtocol out) throws TException {
        kvStore.createTable(dbName, tableName, engineName, properties);
        writeOkStatus(out);
        return out;
    }

    @Override
    public TProtocol dropTable(String dbName, String tableName, Properties properties, TProtocol out) throws TException {
        kvStore.dropTable(dbName, tableName);
        writeOkStatus(out);
        return out;
    }

    @Override
    public TProtocol optimizeTable(String dbName, String tableName, Properties properties, TProtocol out) throws TException {
        kvStore.optimizeTable(dbName, tableName);
        writeOkStatus(out);
        return out;
    }

    @Override
    public TProtocol flushTable(String dbName, String tableName, Properties properties, TProtocol out) throws TException {
        kvStore.flushTable(dbName, tableName);
        writeOkStatus(out);
        return out;
    }

    @Override
    public TProtocol makeCheckpoint(String dbName, String tableName, Properties properties, TProtocol out) throws TException {
        kvStore.makeCheckpoint(dbName, tableName);
        writeOkStatus(out);
        return out;
    }

    @Override
    public TProtocol getDatabases(Properties properties, TProtocol out) throws TException {
        ArrayList<String> databasesList = kvStore.getDatabasesList();
        writeOkStatus(out);
        out.writeList(TType.STRING, databasesList, out::writeString);
        return out;
    }

    @Override
    public TProtocol getTables(String dbName, Properties properties, TProtocol out) throws TException {
        ArrayList<String> tablesList = kvStore.getTablesList(dbName);
        writeOkStatus(out);
        out.writeList(TType.STRING, tablesList, out::writeString);
        return out;
    }

    @Override
    public TProtocol getTableParameters(String dbName, String tableName, Properties properties, TProtocol out) throws TException {
        HashMap<String, Object> engineParams = kvStore.getEngineParameters(dbName, tableName).getAsMap();
        HashMap<String, Object> tableParams = kvStore.getKvTableParameters(dbName, tableName).getAsMap();
        tableParams.putAll(engineParams);
        writeOkStatus(out);
        out.writeMap(TType.STRING, TType.STRING, tableParams, e -> {
            out.writeString(e.getKey());
            out.writeString(e.getValue().toString());
        });
        return out;
    }

    private static void writeOkStatus(TProtocol out) throws TException {
        out.writeByte((byte) 0);
    }

}
