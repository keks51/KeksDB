package com.keks.kv_storage;

import com.keks.kv_storage.conf.Params;
import com.keks.kv_storage.ex.*;
import com.keks.kv_storage.query.Query;
import com.keks.kv_storage.query.QueryIterator;
import com.keks.kv_storage.record.KVRecord;
import com.keks.kv_storage.utils.SimpleScheduler;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;


public class KVStore implements AutoCloseable {

    private final SimpleScheduler scheduler = new SimpleScheduler();

    public final File kvStoreDir;

    private final ConcurrentHashMap<String, KvDatabase> databases;

    private final ReentrantReadWriteLock storeLock = new ReentrantReadWriteLock();

    public KVStore(File kvStoreDir) {
        this.kvStoreDir = kvStoreDir;
        if (!kvStoreDir.exists()) throw new KvStoreDirectoryNotFound(kvStoreDir);
        this.databases = loadDBs(kvStoreDir, scheduler);
    }

    public boolean createDB(String dbName) {
        return withStoreWriteLock(() -> {
            String dbNameUpper = dbName.toUpperCase();
            if (databases.get(dbNameUpper) == null) {
                File dbDir = new File(kvStoreDir, dbName);
                if (dbDir.exists()) throw new RuntimeException();
                if (!dbDir.mkdir()) throw new RuntimeException();
                databases.put(dbNameUpper, new KvDatabase(dbDir, scheduler));
                return true;
            }
            throw new DatabaseAlreadyExistsException(dbName);
        });
    }

    public void dropDB(String dbName) {
        withStoreWriteLockVoid(() ->
                withDbWriteLockVoid(dbName, kvDatabase -> {
                            if (!kvDatabase.databaseTables.isEmpty())
                                throw new DatabaseNotEmptyException(dbName, Collections.list(kvDatabase.databaseTables.keys()));
                            kvDatabase.setDeleted();
                            databases.remove(kvDatabase.dbName);
                            if (!kvDatabase.databaseDir.delete()) throw new RuntimeException();
                        }
                )
        );
    }

    public Params<?> getEngineParameters(String dbName,
                                         String tableName) {
        return withDatabaseCrudLockRead(dbName, kvDatabase ->
                withTableReadLock(kvDatabase, tableName, KvTable::getEngineProperties)
        );
    }

    // TODO not implemented
    public Params<?> getKvTableParameters(String dbName,
                                          String tableName) {
        return withDatabaseCrudLockRead(dbName, kvDatabase ->
                withTableReadLock(kvDatabase, tableName, KvTable::getTableProperties)
        );
    }

    public void dropTable(String dbName,
                          String tableName) {
        withDbWriteLockVoid(dbName, kvDatabase ->
                withTableWriteLockVoid(kvDatabase, tableName, kvTable ->
                        kvDatabase.dropTable(kvTable.tableName)));
    }

    public String createTable(String dbName,
                              String tableName,
                              String engineTypeStr,
                              Properties properties) {
        return withDatabaseCrudLockRead(dbName, kvDatabase ->
                kvDatabase.createTable(tableName, engineTypeStr, properties));
    }


    public void put(String dbName, String tableName, KVRecord kvRecord) {
        withDatabaseCrudLockReadVoid(dbName, kvDatabase ->
                withTableReadLockVoid(kvDatabase, tableName, kvTable -> kvTable.put(kvRecord)));
    }

    public void put(String dbName, String tableName, String key, byte[] data) {
        withDatabaseCrudLockReadVoid(dbName, kvDatabase ->
                withTableReadLockVoid(kvDatabase, tableName, kvTable -> kvTable.put(key, data)));
    }

    public void remove(String dbName, String tableName, String key) {
        withDatabaseCrudLockReadVoid(dbName, kvDatabase ->
                withTableReadLockVoid(kvDatabase, tableName, kvTable -> kvTable.remove(key)));
    }

    public byte[] get(String dbName, String tableName, String key) {
        return withDatabaseCrudLockRead(dbName, kvDatabase ->
                withTableReadLock(kvDatabase, tableName, kvTable -> kvTable.get(key)));
    }

    public QueryIterator getRecords(String dbName, String tableName, Query query) {
        return withDatabaseCrudLockRead(dbName, kvDatabase ->
                withTableReadLock(kvDatabase, tableName, e -> e.getRecords(query)));
    }

    public long getRecordsCnt(String dbName, String tableName, Query query) {
        return withDatabaseCrudLockRead(dbName, kvDatabase ->
                withTableReadLock(kvDatabase, tableName, e -> e.getRecordsCnt(query)));
    }

    public ArrayList<String> getDatabasesList() {
        return withStoreReadLock(() -> {
            databases.keys().asIterator();
            return new ArrayList<>(Collections.list(databases.keys()));
        });
    }

    public ArrayList<String> getTablesList(String dbName) {
        return withDatabaseCrudLockRead(dbName, KvDatabase::getTablesList);
    }

    public void flushTable(String dbName, String tableName) {
        withDatabaseCrudLockReadVoid(dbName, kvDatabase ->
                withTableReadLockVoid(kvDatabase, tableName, KvTable::forceFlush));
    }

    public void makeCheckpoint(String dbName, String tableName) {
        withDatabaseCrudLockReadVoid(dbName, kvDatabase ->
                withTableWriteLockVoid(kvDatabase, tableName, KvTable::makeCheckPoint));
    }

    public void optimizeTable(String dbName, String tableName) {
        withDatabaseCrudLockReadVoid(dbName, kvDatabase ->
                withTableReadLockVoid(kvDatabase, tableName, KvTable::optimize));
    }

    public void close() {
        withStoreWriteLockVoid(() -> {
            for (String dbName : getDatabasesList()) {
                withDbWriteLockVoid(dbName, KvDatabase::close);
            }
        });
    }

    private void withStoreWriteLockVoid(Runnable func) {
        try {
            storeLock.writeLock().lock();
            func.run();
        } finally {
            storeLock.writeLock().unlock();
        }
    }

    private <T> T withStoreWriteLock(Supplier<T> func) {
        try {
            storeLock.writeLock().lock();
            return func.get();
        } finally {
            storeLock.writeLock().unlock();
        }
    }

    private void withStoreReadLockVoid(Runnable func) {
        try {
            storeLock.readLock().lock();
            func.run();
        } finally {
            storeLock.readLock().unlock();
        }
    }

    private <T> T withStoreReadLock(Supplier<T> func) {
        try {
            storeLock.readLock().lock();
            return func.get();
        } finally {
            storeLock.readLock().unlock();
        }
    }

    private void withDatabaseCrudLockReadVoid(String dbName,
                                              Consumer<KvDatabase> func) throws KVStoreException {
        withDatabaseCrudLockRead(dbName, kvDatabase -> {
            func.accept(kvDatabase);
            return null;
        });
    }

    private <T> T withDatabaseCrudLockRead(String dbName,
                                           Function<KvDatabase, T> func) throws KVStoreException {
        String dbNameUpper = dbName.toUpperCase();
        KvDatabase kvDatabase = databases.get(dbNameUpper);

        if (kvDatabase == null) {
            throw new DatabaseNotFoundException(dbName);
        } else {
            try {
                kvDatabase.lockDbCrudAsRead();
                if (kvDatabase.isDeleted()) {
                    throw new DatabaseNotFoundException(dbName);
                } else {
                    return func.apply(kvDatabase);
                }
            } finally {
                kvDatabase.unlockDbCrudAsRead();
            }
        }
    }

    private void withDbWriteLockVoid(String dbName,
                                     Consumer<KvDatabase> func) throws KVStoreException {
        withDbWriteLock(dbName, kvDatabase -> {
            func.accept(kvDatabase);
            return null;
        });
    }

    private <T> T withDbWriteLock(String dbName,
                                  Function<KvDatabase, T> func) throws KVStoreException {
        String dbNameUpper = dbName.toUpperCase();
        KvDatabase kvDatabase = databases.get(dbNameUpper);

        if (kvDatabase == null) {
            throw new DatabaseNotFoundException(dbName);
        } else {
            try {
                kvDatabase.lockDbCrudAsWrite();
                if (kvDatabase.isDeleted()) {
                    throw new DatabaseNotFoundException(dbName);
                } else {
                    return func.apply(kvDatabase);
                }
            } finally {
                kvDatabase.unlockDbCrudAsWrite();
            }
        }
    }

    private void withTableReadLockVoid(KvDatabase kvDatabase,
                                       String table,
                                       Consumer<KvTable> func) throws KVStoreException {
        withTableReadLock(kvDatabase, table, kvTable -> {
            func.accept(kvTable);
            return "";
        });
    }

    private <T> T withTableReadLock(KvDatabase kvDatabase,
                                    String table,
                                    Function<KvTable, T> func) throws KVStoreException {
        String tableNameUpper = table.toUpperCase();
        KvTable kvTable = kvDatabase.databaseTables.get(tableNameUpper);

        if (kvTable == null) {
            throw new TableNotFoundException(kvDatabase.dbName, tableNameUpper);
        } else {
            try {
                kvTable.lockTableAsRead();
                if (kvTable.isDeleted()) {
                    throw new TableNotFoundException(kvDatabase.dbName, tableNameUpper);
                } else {
                    return func.apply(kvTable);
                }
            } finally {
                kvTable.unlockTableAsRead();
            }
        }
    }

    private void withTableWriteLockVoid(KvDatabase kvDatabase,
                                        String table,
                                        Consumer<KvTable> func) throws KVStoreException {
        withTableWriteLock(kvDatabase, table, kvTable -> {
            func.accept(kvTable);
            return "";
        });
    }

    private <T> T withTableWriteLock(KvDatabase kvDatabase,
                                     String table,
                                     Function<KvTable, T> func) throws KVStoreException {
        String tableNameUpper = table.toUpperCase();
        KvTable kvTable = kvDatabase.databaseTables.get(tableNameUpper);

        if (kvTable == null) {
            throw new TableNotFoundException(kvDatabase.dbName, table);
        } else {
            try {
                kvTable.lockTableAsWrite();
                if (kvTable.isDeleted()) {
                    throw new TableNotFoundException(kvDatabase.dbName, table);
                } else {
                    return func.apply(kvTable);
                }
            } finally {
                kvTable.unlockTableAsWrite();
            }
        }
    }

    private static ConcurrentHashMap<String, KvDatabase> loadDBs(File kvStoreDir, SimpleScheduler scheduler) {
        ConcurrentHashMap<String, KvDatabase> databases = new ConcurrentHashMap<>();
        File[] files = kvStoreDir.listFiles();
        if (files == null) throw new KvStoreDirectoryNotFound(kvStoreDir);
        if (files.length > 0) {
            for (File file : files) {
                if (file.isFile()) throw new RuntimeException("Suspicious file exists inside kv store dir: " + file);
            }

            for (File file : files) {
                databases.put(file.getName().toUpperCase(), new KvDatabase(file, scheduler));
            }
        }
        return databases;
    }

}
