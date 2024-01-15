package utils;

import com.keks.kv_storage.conf.TableEngineType;
import com.keks.kv_storage.lsm.io.MetadataJsonRW;
import com.keks.kv_storage.lsm.io.SSTableWriter;
import com.keks.kv_storage.utils.UnCheckedConsumer;
import org.opentest4j.AssertionFailedError;

import java.io.File;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.keks.kv_storage.recovery.RecoveryManager.CHECKPOINT_DIR_NAME;
import static org.junit.jupiter.api.Assertions.*;


public class TestUtils {

    public static void assertCheckpointDirContainsLsmSSTables(File tableDir, String... ssTableIds) {
        File checkpointDir = new File(new File(tableDir, TableEngineType.LSM.toString()), CHECKPOINT_DIR_NAME);
        assertSStablesExists(checkpointDir, ssTableIds);
    }

    public static void assertSStablesExists(File tableDir, String... ssTableDirs) {
        assertTrue(tableDir.exists(), tableDir.getAbsolutePath() + " doesn't exist");
        for (String ssTableDir : ssTableDirs) {
            assertSStableExists(new File(tableDir, ssTableDir));
        }
        assertNumberOfSSTables(tableDir, ssTableDirs.length);
    }

    public static void assertSStableExists(File tableDir) {
        assertTrue(tableDir.exists(), tableDir.getAbsolutePath() + " doesn't exist");

        File dataFile = new File(tableDir, SSTableWriter.DATA_FILE_NAME);
        assertTrue(dataFile.exists(), dataFile.getAbsolutePath() + " doesn't exist");

        File indexFile = new File(tableDir, SSTableWriter.DENSE_INDEX_FILE_NAME);
        assertTrue(indexFile.exists(), indexFile.getAbsolutePath() + " doesn't exist");

        File partialIndexFile = new File(tableDir, SSTableWriter.SPARSE_INDEX_FILE_NAME);
        assertTrue(partialIndexFile.exists(), partialIndexFile.getAbsolutePath() + " doesn't exist");

        File metadataFile = new File(tableDir, MetadataJsonRW.METADATA_FILE_NAME);
        assertTrue(metadataFile.exists(), metadataFile.getAbsolutePath() + " doesn't exist");
    }

    public static void assertNumberOfSSTables(File tableDir, int expectedNumber) {
        ;
        File[] files = tableDir.listFiles(File::isDirectory);
        if (files == null) throw new AssertionFailedError();
        String filesStr = Arrays.stream(files).map(File::getAbsolutePath).collect(Collectors.joining("\n"));
        int actualNumberOfDirs = files.length;
        assertEquals(expectedNumber, actualNumberOfDirs,
                tableDir.getAbsolutePath() + " contains " + actualNumberOfDirs + " dirs instead of " + expectedNumber + "\n" + filesStr);
    }

    public static void assertDirIsEmpty(Path dir) {
        assertDirIsEmpty(dir.toFile());
    }

    public static void assertDirIsEmpty(File dir) {
        File[] files = dir.listFiles();
        if (files == null) throw new AssertionFailedError("Directory doesn't exist: " + dir);
        assertEquals(0, files.length);
    }

    public static void runConcurrentTest(int taskCount,
                                         int threadPoolAwaitTimeoutSec,
                                         int taskAwaitTimeoutSec,
                                         Function<Integer, String> function,
                                         int numberOfThreads) throws InterruptedException, ExecutionException, TimeoutException {
        ExecutorService executor = Executors.newFixedThreadPool(numberOfThreads);
        List<Future<?>> futures = new ArrayList<>(taskCount);
        for (int i = 0; i < taskCount; i++) {
            int y = i;
            Future<?> future1 = executor.submit(() -> function.apply(y));
            futures.add(future1);
        }
        executor.shutdown();
        if (!executor.awaitTermination(threadPoolAwaitTimeoutSec, TimeUnit.SECONDS)) {
            throw new InterruptedException();
        }

        assertEquals(taskCount, futures.size());
        for (Future<?> future : futures) {
            future.get(taskAwaitTimeoutSec, TimeUnit.SECONDS);
        }

    }


//    public static Timer timer421 = BPlusKVTable.registry.timer("timer421");

    public static <EX extends Exception> void runConcurrentTest(int taskCount,
                                                                int threadPoolAwaitTimeoutSec,
                                                                int taskAwaitTimeoutSec,
                                                                UnCheckedConsumer<Integer, EX> function,
                                                                int numberOfThreads) throws InterruptedException, ExecutionException, TimeoutException, EX {
        ExecutorService executor = Executors.newFixedThreadPool(numberOfThreads);
        List<Future<?>> futures = new ArrayList<>(taskCount);
        for (int i = 0; i < taskCount; i++) {
            int y = i;
            Future<?> future1 = executor.submit(() -> {
                try {
                    function.accept(y);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
            futures.add(future1);
        }
        executor.shutdown();
        if (!executor.awaitTermination(threadPoolAwaitTimeoutSec, TimeUnit.SECONDS)) {
            throw new InterruptedException();
        }

        assertEquals(taskCount, futures.size());
        for (Future<?> future : futures) {
            future.get(taskAwaitTimeoutSec, TimeUnit.SECONDS);
        }

    }

    public static void emptyDir(File dir) {
        File[] files = dir.listFiles();
        for (File file : files) {
            deleteDirectory(file);
        }
    }

    public static boolean deleteDirectory(File directoryToBeDeleted) {
        File[] allContents = directoryToBeDeleted.listFiles();
        if (allContents != null) {
            for (File file : allContents) {
                deleteDirectory(file);
            }
        }
        return directoryToBeDeleted.delete();
    }

    public static <E> void iterateAndAssert(Iterator<E> exp, Iterator<E> act) {
        List<E> expList = new LinkedList<>();
        List<E> actList = new LinkedList<>();
        int expSize = 0;
        int actSize = 0;
        while (true) {
            boolean expHasNext = exp.hasNext();
            boolean actHasNext = act.hasNext();
            if (expHasNext && !actHasNext) {
                String expStr = expList.stream().map(n -> String.valueOf(n)).collect(Collectors.joining("\n", "", "\n"));
                String actStr = actList.stream().map(n -> String.valueOf(n)).collect(Collectors.joining("\n", "", "\n"));
                throw new org.opentest4j.AssertionFailedError(
                        "Expected iterator (size=" + (expSize + 1) + ") is bigger than Actual iterator (size=" + actSize + ")\n"
                                + "Exp:\n" + expStr + "Act:\n" + actStr
                );
            }
            if (!expHasNext && actHasNext) {
                String expStr = expList.stream().map(n -> String.valueOf(n)).collect(Collectors.joining("\n", "", "\n"));
                String actStr = actList.stream().map(n -> String.valueOf(n)).collect(Collectors.joining("\n", "", "\n"));
                throw new org.opentest4j.AssertionFailedError(
                        "Actual iterator (size=" + (actSize + 1) + ") is bigger than Expected iterator (size=" + expSize + ")\n"
                                + "Exp:\n" + expStr + "Act:\n" + actStr);
            }
            if (!expHasNext) break;
            expSize++;
            actSize++;

            E expNext = exp.next();
            E actNext = act.next();
            assertEquals(expNext, actNext);
            expList.add(expNext);
            actList.add(actNext);
        }

    }


}
