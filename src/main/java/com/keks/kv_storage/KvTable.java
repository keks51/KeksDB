package com.keks.kv_storage;

import com.keks.kv_storage.bplus.buffer.PageBuffer;
import com.keks.kv_storage.bplus.conf.BPlusConf;
import com.keks.kv_storage.bplus.BPlusEngine;
import com.keks.kv_storage.kv_table_conf.KvTableConf;
import com.keks.kv_storage.conf.Params;
import com.keks.kv_storage.conf.TableEngine;
import com.keks.kv_storage.conf.TableEngineType;
import com.keks.kv_storage.ex.*;
import com.keks.kv_storage.io.JsonFileRW;
import com.keks.kv_storage.lsm.conf.LsmConf;
import com.keks.kv_storage.lsm.LsmEngine;
import com.keks.kv_storage.query.Query;
import com.keks.kv_storage.query.QueryIterator;
import com.keks.kv_storage.record.KVRecord;
import com.keks.kv_storage.recovery.CommitLogAppender;
import com.keks.kv_storage.recovery.JournalEvent;
import com.keks.kv_storage.recovery.RecoveryJournal;
import com.keks.kv_storage.recovery.RecoveryManager;
import com.keks.kv_storage.utils.SimpleScheduler;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.locks.ReentrantReadWriteLock;


public class KvTable {

    public final String keyFirstCol;
    public final String valueFirstCol;
    public final ArrayList<String> keyColumns;
    public final ArrayList<String> valueColumns;
    public final HashMap<String, String> keySchema;
    public final HashMap<String, String> valueSchema;

    public final String tableId;
    private final String logPrefix;
    private ScheduledFuture<?> checkPointMaker;

    static final Logger log = LogManager.getLogger(KvTable.class.getName());

    public static final String KV_TABLE_CONF_FILE = "kv-table.conf";

    public final KvTableConf kvTableConf;
    private CommitLogAppender commitLogAppender;
    private volatile boolean isDeleted;

    public final String dbName;
    public final String tableName;
    private final SimpleScheduler scheduler;

    private final TableEngine tableEngine;

    private final RecoveryManager recoveryManager;
    public final File engineDir;

    private final ReentrantReadWriteLock tableLock = new ReentrantReadWriteLock();

    private final String dbTbName;

    public KvTable(String tableId,
                   String dbName,
                   String tableName,
                   File engineDir,
                   TableEngine tableEngine,
                   RecoveryManager recoveryManager,
                   KvTableConf kvTableConf,
                   CommitLogAppender commitLogAppender,
                   SimpleScheduler scheduler) throws IOException {
        this(
                tableId,
                dbName,
                tableName,
                engineDir,
                tableEngine,
                recoveryManager,
                kvTableConf,
                commitLogAppender,
                scheduler,
                new ArrayList<>() {{
                    add("key");
                }},
                new HashMap<>() {{
                    put("key", "string");
                }},
                new ArrayList<>() {{
                    add("value");
                }},
                new HashMap<>() {{
                    put("value", "string");
                }});
    }

    public KvTable(String tableId,
                   String dbName,
                   String tableName,
                   File engineDir,
                   TableEngine tableEngine,
                   RecoveryManager recoveryManager,
                   KvTableConf kvTableConf,
                   CommitLogAppender commitLogAppender,
                   SimpleScheduler scheduler,
                   ArrayList<String> keyColumns,
                   HashMap<String, String> keySchema,
                   ArrayList<String> valueColumns,
                   HashMap<String, String> valueSchema
                   ) throws IOException {
        this.tableId = tableId;
        this.logPrefix = "[ tableId: " + tableId + " ]";
        this.engineDir = engineDir;
        this.kvTableConf = kvTableConf;
        this.dbName = dbName;
        this.tableName = tableName;
        this.scheduler = scheduler;
        this.dbTbName = dbName + "." + tableName;
        this.tableEngine = tableEngine;
        this.recoveryManager = recoveryManager;
        this.commitLogAppender = commitLogAppender;
        this.keyFirstCol = keyColumns.get(0);
        this.keyColumns = keyColumns;
        this.keySchema = keySchema;
        this.valueFirstCol = valueColumns.get(0);
        this.valueColumns = valueColumns;
        this.valueSchema = valueSchema;
    }

    public void makeCheckPoint() {
        try {
            log.info(logPrefix + " Creating checkpoint");
            tableEngine.forceFlush();
            commitLogAppender.close();
            recoveryManager.createCheckPoint();

            commitLogAppender = new CommitLogAppender(recoveryManager.commitLogDir, kvTableConf.commitLogParallelism);
            log.info(logPrefix + " Created checkpoint");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void forceFlush() {
        try {
            log.info(dbTbName + " Force flushing");
            tableEngine.forceFlush();
            log.info(dbTbName + " Force flushed");
        } catch (IOException e) {
            throw new FlushException(e);
        }
    }

    public void optimize() {
        try {
            log.info(dbTbName + " Optimizing");
            tableEngine.optimize();
            log.info(dbTbName + " Optimized");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void put(KVRecord kvRecord) {
        if (kvRecord.key.isEmpty()) throw new EmptyKeyException();
        if (kvRecord.valueBytes.length == 0) throw new EmptyValueException();
        try {
            commitLogAppender.append(kvRecord);
            tableEngine.put(kvRecord);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void put(String key, byte[] data) {
        if (key.isEmpty()) throw new EmptyKeyException();
        if (data.length == 0) throw new EmptyValueException();
        try {
            KVRecord kvRecord = new KVRecord(key, data);
            commitLogAppender.append(kvRecord);
            tableEngine.put(kvRecord);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

//    public void put(HashMap<String, Byte[]> keyData, HashMap<String, Byte[]> valueData) {
//
//        if (keyData.isEmpty() || keyData.get(keyFirstCol).length == 0) throw new EmptyKeyException();
//        if (valueData.isEmpty() || valueData.get(valueFirstCol).length == 0) throw new EmptyValueException();
//        try {
//
//            KVRecord kvRecord = new KVRecord(key, data);
//            commitLogAppender.append(dbName, tableName, kvRecord);
//            tableEngine.put(kvRecord);
//        } catch (IOException e) {
//            throw new RuntimeException(e);
//        }
//    }

    public void remove(String key) {
        if (key.isEmpty()) throw new EmptyKeyException();
        try {
            KVRecord kvRecord = new KVRecord(key, "".getBytes());
            commitLogAppender.append(kvRecord);
            tableEngine.remove(key);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public byte[] get(String key) {
        if (key.isEmpty()) throw new EmptyKeyException();
        try {
            KVRecord kvRecord = tableEngine.get(key);
            if (kvRecord != null) return kvRecord.valueBytes;
            return null;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public QueryIterator getRecords(Query query) {
        try {
            return tableEngine.getRangeRecords(query);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public long getRecordsCnt(Query query) {
        try {
            return tableEngine.getRecordsCnt(query);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public Params<?> getEngineProperties() {
        return tableEngine.getTableProperties();
    }

    public Params<?> getTableProperties() {
        return kvTableConf;
    }

    public void close() {
        try {
            log.info(dbTbName + " Closing");
            if (checkPointMaker != null) checkPointMaker.cancel(false);
            tableEngine.forceFlush();
            recoveryManager.createCheckPoint();
            tableEngine.close();
            recoveryManager.close();
            log.info(dbTbName + " Closed");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static KvTable loadTable(String dbName,
                                    String tableName,
                                    File tableDir,
                                    SimpleScheduler scheduler) {
        try {
            String tableId = dbName + "." + tableName;
            log.info("Loading " + tableId + " from path: " + tableDir);
            File engineDir = getEngineDir(tableDir);
            File recoveryJournalFile = new File(engineDir, RecoveryJournal.RECOVERY_JOURNAL_FILENAME);
            if (!recoveryJournalFile.exists()) throw new RecoveryJournalFileNotFoundException(recoveryJournalFile);

            KvTableConf kvTableConf = new KvTableConf(JsonFileRW.read(new File(engineDir, KV_TABLE_CONF_FILE)));
            RecoveryJournal recoveryJournal = new RecoveryJournal(recoveryJournalFile);
            RecoveryManager recoveryManager = new RecoveryManager(
                    tableId,
                    engineDir,
                    kvTableConf.commitLogParallelism,
                    recoveryJournal);

            boolean needRecovery = recoveryManager.prepareDataForRecoveryIfNeeded();

            TableEngineType tableEngineType = TableEngineType.valueOf(engineDir.getName().toUpperCase());
            TableEngine tableEngine;
            switch (tableEngineType) {
                case BPLUS:
                    tableEngine = BPlusEngine.loadTable(tableId, recoveryManager.dataDir);
                    break;
                case LSM:
                    tableEngine = LsmEngine.loadTable(tableId, recoveryManager.dataDir, scheduler);
                    break;
                default:
                    throw new RuntimeException();
            }

            if (needRecovery) recoveryManager.recover2(tableEngine);

            JsonFileRW.write(new File(tableDir, KV_TABLE_CONF_FILE), kvTableConf.getAsJson());

            return new KvTable(
                    tableId,
                    dbName,
                    tableName,
                    engineDir,
                    tableEngine,
                    recoveryManager,
                    kvTableConf,
                    new CommitLogAppender(recoveryManager.commitLogDir, kvTableConf.commitLogParallelism),
                    scheduler);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    // TODO
    public String getTableConf() {
        return "";
    }

    public static KvTable createTable(String dbName,
                                      String tableName,
                                      File engineDir,
                                      TableEngineType engineType,
                                      Properties properties,
                                      SimpleScheduler scheduler) throws IOException {
        String tableId = dbName + "." + tableName;

        KvTableConf kvTableConf = new KvTableConf(properties);
        JsonFileRW.write(new File(engineDir, KV_TABLE_CONF_FILE), kvTableConf.getAsJson());

        File dataDir = new File(engineDir, RecoveryManager.DATA_DIR_NAME);
        if (!dataDir.mkdir()) throw new CannotCreateDirException(dataDir);
        TableEngine tableEngine;
        switch (engineType) {
            case BPLUS:
                BPlusConf bplusConf = new BPlusConf(properties);
                tableEngine = BPlusEngine.createNewTable(tableId, dataDir, bplusConf);
                break;
            case LSM:
                LsmConf lsmConf = new LsmConf(properties);
                tableEngine = LsmEngine.createNewTable(tableId, dataDir, lsmConf, scheduler);
                break;
            default:
                throw new RuntimeException();

        }
        RecoveryJournal recoveryJournal = RecoveryJournal.create(engineDir);
        recoveryJournal.writeEvent(JournalEvent.CREATING_TABLE);
        RecoveryManager recoveryManager = new RecoveryManager(
                dbName + "." + tableName,
                engineDir,
                kvTableConf.commitLogParallelism,
                recoveryJournal);

        recoveryManager.createCheckPoint();
        return new KvTable(
                tableId,
                dbName,
                tableName,
                engineDir,
                tableEngine,
                recoveryManager,
                kvTableConf,
                new CommitLogAppender(recoveryManager.commitLogDir, kvTableConf.commitLogParallelism),
                scheduler);
    }

    public void setCheckPointMakerThread(ScheduledFuture<?> checkPointMaker) {
        this.checkPointMaker = checkPointMaker;
    }

    private static File getEngineDir(File tableDir) {
        File[] files = tableDir.listFiles(File::isDirectory);
        if (files == null || files.length == 0) throw new EngineDirNotFoundException(tableDir);
        return files[0];
    }

    public void lockTableAsRead() {
        tableLock.readLock().lock();
    }

    public void unlockTableAsRead() {
        tableLock.readLock().unlock();
    }

    public void lockTableAsWrite() {
        log.info(logPrefix + " trying to lock write");
        tableLock.writeLock().lock();
        log.info(logPrefix + " locked write");
    }

    public void unlockTableAsWrite() {
        log.info(logPrefix + " trying to unlock write");
        tableLock.writeLock().unlock();
        log.info(logPrefix + " unlocked write");
    }

    public boolean isDeleted() {
        return isDeleted;
    }

    public void setDeleted() {
        isDeleted = true;
    }

}
