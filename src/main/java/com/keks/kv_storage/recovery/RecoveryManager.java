package com.keks.kv_storage.recovery;

import com.keks.kv_storage.record.KVRecord;
import com.keks.kv_storage.conf.TableEngine;
import com.keks.kv_storage.ex.CannotCreateDirException;
import com.keks.kv_storage.ex.RecoveryManagerException;
import com.keks.kv_storage.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;


public class RecoveryManager implements Closeable {

    static final Logger log = LogManager.getLogger(RecoveryManager.class.getName());

    public static final String COMMIT_LOG_FILE_NAME = "commit-log.db";
    public static final String COMMIT_LOG_DIR_NAME = "commit_log";

    public static final String DATA_DIR_NAME = "data";
    public static final String CHECKPOINT_DIR_NAME = "checkpoint";


    public final File checkPointDir;
    public final File dataDir;

    public final File tableDir;

    public final File commitLogDir;
    private final int commitLogParalelism;
    private final String tableName;
    private final RecoveryJournal recoveryJournal;

    public RecoveryManager(String tableName,
                           File tableDir,
                           int commitLogParalelism,
                           RecoveryJournal recoveryJournal) throws IOException {
        this.tableDir = tableDir;
        this.tableName = tableName;
        this.commitLogDir = new File(tableDir, COMMIT_LOG_DIR_NAME);
        this.checkPointDir = new File(tableDir, CHECKPOINT_DIR_NAME);
        this.dataDir = new File(tableDir, DATA_DIR_NAME);
        this.commitLogParalelism = commitLogParalelism;
        this.recoveryJournal = recoveryJournal;
    }

    public boolean needRecovery() throws IOException {
        JournalEvent journalEvent = recoveryJournal.readEvent();
        return journalEvent != JournalEvent.END;
    }

    public void createCheckPoint() throws IOException {
        log.info(tableName + " creating checkpoint");
        recoveryJournal.writeEvent(JournalEvent.CREATING_CHECKPOINT);
        deleteCommitDir();
        deleteCheckPointDir();
        copyDataToCheckPoint();
        recoveryJournal.writeEvent(JournalEvent.RUNNING);
    }

    private void recoverDataFromCheckpoint() throws IOException {
        recoveryJournal.writeEvent(JournalEvent.RECOVERING_DATA_FROM_CHECKPOINT);
        deleteDataDir();
        copyCheckPointToData();
    }

    public boolean prepareDataForRecoveryIfNeeded() throws IOException {
        JournalEvent journalEvent = recoveryJournal.readEvent();
        if (journalEvent == JournalEvent.END) {
            log.info(tableName + " doesn't need recovery");
            return false;
        } else {
            switch (journalEvent) {
                // failed during creation
                case CREATING_TABLE:
                    log.info(tableName + " was failed during creation. doesn't need recovery");
                    deleteDataDir();
                    deleteCheckPointDir();
                    deleteCommitDir();
                    if (!dataDir.mkdir()) throw new CannotCreateDirException(dataDir);
                    if (!checkPointDir.mkdir()) throw new CannotCreateDirException(checkPointDir);
                    recoveryJournal.writeEvent(JournalEvent.END);
                    return false;

                case RUNNING:
                case RECOVERING_DATA_FROM_CHECKPOINT:
                    log.info(tableName + " was failed during running. needs to recover from commit log");
                    recoverDataFromCheckpoint();
                    return true;

                // failed during  checkpoint process
                case CREATING_CHECKPOINT:
                    log.info(tableName + " was failed during creating checkpoint. doesn't need to recover from commit log");
                    createCheckPoint();
                    return false;

                default:
                    throw new IllegalArgumentException("Unknown recovery journal event: " + journalEvent);
            }
        }
    }

    private void deleteCheckPointDir() throws IOException {
        log.info(tableName + " deleting checkpoint dir: " + checkPointDir);
        if (checkPointDir.exists() && !FileUtils.deleteDirectory(checkPointDir)) {
            throw new RecoveryManagerException("Cannot delete check point dir: " + checkPointDir);
        }
    }

    private void copyDataToCheckPoint() throws IOException {
        log.info(tableName + " copying data dir to checkpoint tmp: " + dataDir + " to " + checkPointDir);
        FileUtils.copyFolder(dataDir, checkPointDir);
    }

    private void copyCheckPointToData() throws IOException {
        log.info(tableName + " copying checkpoint dir to data dir: " + checkPointDir + " to " + dataDir);
        FileUtils.copyFolder(checkPointDir, dataDir);
    }

    private void deleteDataDir() throws IOException {
        log.info(tableName + " deleting data dir: " + dataDir);
        if (dataDir.exists()) {
            if (!FileUtils.deleteDirectory(dataDir)) {
                throw new RecoveryManagerException("Cannot delete check point dir: " + dataDir);
            }
        }
    }

    public void deleteCommitDir() throws IOException {
        log.info(tableName + " deleting commit log dir: " + commitLogDir);
        if (commitLogDir.exists() && !FileUtils.deleteDirectory(commitLogDir)) {
            throw new RecoveryManagerException("Cannot delete check point dir tmp: " + commitLogDir);
        }
    }

    public void recover2(TableEngine tableEngine) throws IOException {
        log.info("Table '" + tableName + "' recovering from " + commitLogParalelism + " files");
        CommitLogReader commitLogReader = new CommitLogReader(commitLogDir, commitLogParalelism);
        int addCnt = 0;
        int deleteCnt = 0;
        while (commitLogReader.hasNext()) {
            KVRecord record = commitLogReader.next().kvRecord;
            if (record.isDeleted()) {
                tableEngine.remove(record.key);
                deleteCnt++;
            } else {
                tableEngine.put(record);
                addCnt++;
            }
        }

        log.info("Table '" + tableName + "' successfully recovered. Added: " + addCnt + " Deleted: " + deleteCnt);
        commitLogReader.close();
        tableEngine.forceFlush();
        createCheckPoint();
    }

    @Override
    public void close() throws IOException {
        recoveryJournal.writeEvent(JournalEvent.END);
        recoveryJournal.close();
    }
}
