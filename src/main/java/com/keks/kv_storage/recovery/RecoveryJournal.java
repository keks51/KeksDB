package com.keks.kv_storage.recovery;


import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

public class RecoveryJournal implements Closeable {


    public static String RECOVERY_JOURNAL_FILENAME = "recovery-journal.db";
    private final RandomAccessFile raf;


    public RecoveryJournal(File file) throws IOException {
        this.raf = new RandomAccessFile(file, "rw");
    }

    public void writeEvent(JournalEvent event) throws IOException {
        raf.seek(0);
        raf.writeUTF(event.name());
    }

    public JournalEvent readEvent() throws IOException {
        raf.seek(0);
        return JournalEvent.valueOf(raf.readUTF());
    }


    @Override
    public void close() throws IOException {
        raf.close();
    }

    public static RecoveryJournal create(File dir) throws IOException {
        File file = new File(dir, RECOVERY_JOURNAL_FILENAME);
        assert !file.exists();
        return new RecoveryJournal(file);
    }
}
