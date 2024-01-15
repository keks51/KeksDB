package com.keks.kv_storage;

import com.keks.kv_storage.conf.TableEngineType;
import com.keks.kv_storage.ex.CannotCreateDirException;
import com.keks.kv_storage.ex.TableAlreadyExistsException;
import com.keks.kv_storage.ex.TableDirectoryNotEmptyException;
import com.keks.kv_storage.io.FileUtils;
import com.keks.kv_storage.utils.SimpleScheduler;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.locks.ReentrantReadWriteLock;


public class KvDatabase {

    static final Logger log = LogManager.getLogger(KvDatabase.class.getName());

    private final String logPrefix;
    private volatile boolean isDeleted;

    public final String dbName;

    private final ReentrantReadWriteLock dbLock = new ReentrantReadWriteLock();

    public final ConcurrentHashMap<String, KvTable> databaseTables;

    public final File databaseDir;
    private final SimpleScheduler scheduler;

    public KvDatabase(File databaseDir, SimpleScheduler scheduler) {
        this.dbName = databaseDir.getName().toUpperCase();
        this.databaseDir = databaseDir;
        this.scheduler = scheduler;
        this.databaseTables = loadTables(databaseDir, scheduler);
        this.logPrefix = "[ DB: " + dbName + " ]";
    }

    public String createTable(String tableName,
                              String engineTypeStr,
                              Properties properties) {
        try {
            String tableNameUpper = tableName.toUpperCase();
            if (databaseTables.get(tableNameUpper) != null) throw new TableAlreadyExistsException(dbName, tableName);
            TableEngineType tableEngineType = TableEngineType.valueOf(engineTypeStr.toUpperCase());
            File tableDir = new File(databaseDir, tableName);
            if (tableDir.exists()) throw new TableAlreadyExistsException(dbName, tableName);
            if (!tableDir.mkdir()) throw new CannotCreateDirException(tableDir);
            File engineDir = new File(tableDir, tableEngineType.name().toLowerCase());
            if (!engineDir.mkdir()) throw new CannotCreateDirException(engineDir);

            KvTable kvTable = KvTable.createTable(dbName, tableName, engineDir, tableEngineType, properties, scheduler);
            databaseTables.put(tableNameUpper, kvTable);
            if (kvTable.kvTableConf.enableCheckpoint) {
                ScheduledFuture<?> checkPointMaker = scheduler.scheduleWithFixedDelaySecHighPriority(
                        () -> {
                            kvTable.lockTableAsWrite();
                            kvTable.makeCheckPoint();
                            kvTable.unlockTableAsWrite();
                        },
                        kvTable.kvTableConf.triggerCheckpointAfterSec,
                        kvTable.kvTableConf.triggerCheckpointAfterSec
                );
                kvTable.setCheckPointMakerThread(checkPointMaker);
                log.info(logPrefix + "Starting Background checkpointing for table: " + kvTable.tableId
                        + " each " + kvTable.kvTableConf.triggerCheckpointAfterSec);
            } else {
                log.info(logPrefix + "Background checkpointing is disabled");
            }

            return kvTable.getTableConf();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    public void dropTable(String tableName) {
        String tableNameUpper = tableName.toUpperCase();
        KvTable kvTable = databaseTables.get(tableNameUpper);
        kvTable.setDeleted();
        databaseTables.remove(tableNameUpper);
        if (!FileUtils.deleteDirectory(kvTable.engineDir.getParentFile()))
            throw new TableDirectoryNotEmptyException(dbName, tableName, kvTable.engineDir);
    }

    public ArrayList<String> getTablesList() {
        return new ArrayList<>(Collections.list(databaseTables.keys()));
    }

    private static ConcurrentHashMap<String, KvTable> loadTables(File databaseDir, SimpleScheduler scheduler) {
        ConcurrentHashMap<String, KvTable> databaseTables = new ConcurrentHashMap<>();
        File[] tableDirs = databaseDir.listFiles(File::isDirectory);
        if (tableDirs == null) throw new RuntimeException();
        String dbName = databaseDir.getName().toUpperCase();
        for (File tableDir : tableDirs) {
            String tableName = tableDir.getName().toUpperCase();
            KvTable kvTable = KvTable.loadTable(dbName, tableName, tableDir, scheduler);
            databaseTables.put(tableName, kvTable);
        }
        return databaseTables;
    }

    public void close() {
        for (Map.Entry<String, KvTable> stringKvTableEntry : databaseTables.entrySet()) {
            KvTable kvTable = stringKvTableEntry.getValue();
            kvTable.close();

            databaseTables.remove(stringKvTableEntry.getKey());
        }
        log.info("Database '" + dbName + "' was closed");
    }

    public void lockDbCrudAsRead() {
        dbLock.readLock().lock();
    }

    public void unlockDbCrudAsRead() {
        dbLock.readLock().unlock();
    }

    public void lockDbCrudAsWrite() {
        dbLock.writeLock().lock();
    }

    public void unlockDbCrudAsWrite() {
        dbLock.writeLock().unlock();
    }

    public boolean isDeleted() {
        return isDeleted;
    }

    public void setDeleted() {
        isDeleted = true;
    }

}
