package com.keks.kv_storage.ex;

import com.keks.kv_storage.recovery.RecoveryJournal;

import java.io.File;


public class RecoveryJournalFileNotFoundException extends KVStoreException {

    public RecoveryJournalFileNotFoundException(File file) {
        super("File: " + RecoveryJournal.RECOVERY_JOURNAL_FILENAME + " not found in path: " + file);
    }

}
