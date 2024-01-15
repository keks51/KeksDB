package com.keks.kv_storage.client;


import com.keks.kv_storage.conf.TableEngineType;
import com.keks.kv_storage.ex.ThriftClientCommandException;
import com.keks.kv_storage.ex.ThriftClientException;
import com.keks.kv_storage.server.KVStorageCommand;
import com.keks.kv_storage.utils.UnCheckedFunction;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.*;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import java.nio.ByteBuffer;
import java.util.*;

import static com.keks.kv_storage.conf.ConfigParams.*;
import static com.keks.kv_storage.server.KVStorageCommand.*;


public class KVThriftClient {


    @FunctionalInterface
    interface UnCheckedConsumer<T, EX extends Exception> {
        void accept(T t) throws EX;
    }

    private static final byte NEXT_MESSAGE = 0;
    private static final byte SUCCESS = 0;
    private static final byte FAILED = 1;

    private final TBinaryProtocol protocol;

    public KVThriftClient(String host, int port) throws TTransportException {
        TTransport transport = new TSocket(host, port);
        transport.open();
        this.protocol = new TBinaryProtocol(transport);
    }

    public void createDB(String db) throws TException {
        withExceptionHandler(CREATE_DB, protocol -> {
            addProperties(new HashMap<>() {{
                put(DATABASE_NAME, db);
            }});
        });
    }

    public void dropDB(String db) throws TException {
        withExceptionHandler(DROP_DB, protocol -> {
            addProperties(new HashMap<>() {{
                put(DATABASE_NAME, db);
            }});
        });
    }

    public void createTable(String db,
                            String table,
                            TableEngineType engine,
                            HashMap<String, Object> params) throws TException {
        withExceptionHandler(CREATE_TABLE, protocol -> {
            HashMap<String, Object> properties = new HashMap<>() {
                {
                    put(DATABASE_NAME, db);
                    put(TABLE_NAME, table);
                    put(ENGINE_NAME, engine.name().toLowerCase());
                }
            };
            properties.putAll(params);
            addProperties(properties);
        });
    }

    public void dropTable(String db, String table) throws TException {
        withExceptionHandler(DROP_TABLE, protocol -> {
            addProperties(new HashMap<>() {{
                put(DATABASE_NAME, db);
                put(TABLE_NAME, table);
            }});
        });
    }


    public void optimizeTable(String db, String table) throws TException {
        withExceptionHandler(OPTIMIZE_TABLE, protocol -> {
            addProperties(new HashMap<>() {{
                put(DATABASE_NAME, db);
                put(TABLE_NAME, table);
            }});
        });
    }

    public void flushTable(String db, String table) throws TException {
        withExceptionHandler(FLUSH_TABLE, protocol -> {
            addProperties(new HashMap<>() {{
                put(DATABASE_NAME, db);
                put(TABLE_NAME, table);
            }});
        });
    }

    public void createCheckpoint(String db, String table) throws TException {
        withExceptionHandler(MAKE_CHECKPOINT, protocol -> {
            addProperties(new HashMap<>() {{
                put(DATABASE_NAME, db);
                put(TABLE_NAME, table);
            }});
        });
    }

    public List<String> getDatabases() throws TException {
        return withExceptionHandler(GET_DATABASES,
                protocol -> addProperties(new HashMap<>()), // TODO looks terrible
                protocol -> protocol.readList(protocol::readString, ArrayList::new));
    }

    public List<String> getTables(String db) throws TException {
        return withExceptionHandler(GET_TABLES,
                protocol -> addProperties(new HashMap<>() {{
                    put(DATABASE_NAME, db);
                }}), // TODO looks terrible
                protocol -> protocol.readList(protocol::readString, ArrayList::new));
    }

    public void putEntity(String db,
                          String table,
                          String key,
                          String value) throws TException {
        withExceptionHandler(PUT_ENTITY,
                protocol -> {
                    protocol.writeString(db);
                    protocol.writeString(table);
                    protocol.writeString(key);
                    protocol.writeBinary(ByteBuffer.wrap(value.getBytes()));
                });
    }

    public void putEntity(String db,
                          String table,
                          String key,
                          byte[] value) throws TException {
        withExceptionHandler(PUT_ENTITY,
                protocol -> {
                    protocol.writeString(db);
                    protocol.writeString(table);
                    protocol.writeString(key);
                    protocol.writeBinary(ByteBuffer.wrap(value));
                });
    }

    public void removeEntity(String db,
                             String table,
                             String key) throws TException {
        withExceptionHandler(REMOVE_ENTITY,
                protocol -> {
                    protocol.writeString(db);
                    protocol.writeString(table);
                    protocol.writeString(key);
                });
    }

    public byte[] getEntity(String db,
                            String table,
                            String key) throws TException {
        return withExceptionHandler(GET_ENTITY,
                protocol -> {
                    protocol.writeString(db);
                    protocol.writeString(table);
                    protocol.writeString(key);
                },
                protocol -> {
                    byte[] array = protocol.readBinary().array();
                    return array.length == 0 ? null : array;
                });
    }

    //
    private void addProperties(Map<String, Object> map) throws TException {
        protocol.writeMap(
                TType.STRING,
                TType.STRING,
                map,
                e -> {
                    protocol.writeString(e.getKey());
                    protocol.writeString(String.valueOf(e.getValue()));
                }
        );
    }

    // TODO same as below
    private void withExceptionHandler(KVStorageCommand command,
                                      UnCheckedConsumer<TBinaryProtocol, TException> func) throws TException {
        protocol.writeByte(NEXT_MESSAGE);
        protocol.writeString(command.name());
        func.accept(protocol);
        protocol.getTransport().flush();
        byte i = protocol.readByte();
        switch (i) {
            case SUCCESS:
                break;
            case FAILED:
                String classException = protocol.readString();
                String responseStr = protocol.readString();
                throw new ThriftClientCommandException(classException, responseStr);
            default:
                throw new ThriftClientException("Unknown response type: " + i);
        }
    }

    private <T> T withExceptionHandler(KVStorageCommand command,
                                       UnCheckedConsumer<TBinaryProtocol, TException> addProtocolPropFunc,
                                       UnCheckedFunction<TBinaryProtocol, T, TException> handleResponseFunc) throws TException {
        protocol.writeByte(NEXT_MESSAGE);
        protocol.writeString(command.name());
        addProtocolPropFunc.accept(protocol);
        protocol.getTransport().flush();
        byte i = protocol.readByte();
        switch (i) {
            case SUCCESS:
                return handleResponseFunc.apply(protocol);
            case FAILED:
                String classException = protocol.readString();
                String responseStr = protocol.readString();
                throw new ThriftClientCommandException(classException, responseStr);
            default:
                throw new ThriftClientException("Unknown response type: " + i);
        }
    }

    public void close() throws TTransportException {
        protocol.getTransport().flush();
        protocol.getTransport().close();
    }
}
