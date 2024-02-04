package com.keks.kv_storage.recovery;

import com.keks.kv_storage.record.KVRecord;
import com.keks.kv_storage.utils.Time;
import com.keks.kv_storage.utils.UnCheckedConsumer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;


class CommitLogChannelTest {

    @Test
    public void test1(@TempDir Path path) throws IOException {
        File file = path.resolve("test.file").toFile();
        int records = 50;
        HashSet<String> exp = new HashSet<>();
        {
            FileChannel fileChannel = FileChannel.open(
                    file.toPath(),
                    StandardOpenOption.SYNC, // DSYNC not always persist data
                    StandardOpenOption.CREATE_NEW,
                    StandardOpenOption.WRITE);
            CommitLogChannel commitLogChannel = new CommitLogChannel(fileChannel);

            for (int i = 0; i < records; i++) {
                String key = "key" + i;
                exp.add(key);
                ByteBuffer bb = ByteBuffer.allocate(4 + key.length());
                bb.putInt(key.length());
                bb.put(key.getBytes());
                commitLogChannel.write(bb.array());
            }
        }

        {
            FileChannel fileChannel = FileChannel.open(
                    file.toPath(),
                    StandardOpenOption.SYNC, // DSYNC not always persist data
                    StandardOpenOption.CREATE_NEW,
                    StandardOpenOption.READ);
            for (int i = 0; i < records; i++) {
                ByteBuffer intBB = ByteBuffer.allocate(4);
                fileChannel.read(intBB);
                intBB.position(0);
                int size = intBB.getInt();
                ByteBuffer strBB = ByteBuffer.allocate(size);
                fileChannel.read(strBB);
                String key = new String(strBB.array());
                System.out.println(key);
                assertTrue(exp.contains(key));
            }
        }


    }

    @Test
    public void test2(@TempDir Path path) throws Exception {
        File file = path.resolve("test.file").toFile();
        int records = 100_000;
        ConcurrentHashMap.KeySetView<String, Integer> exp = new ConcurrentHashMap<String, Integer>().keySet(1);

        {
            FileChannel fileChannel = FileChannel.open(
                    file.toPath(),
                    StandardOpenOption.SYNC, // DSYNC not always persist data
                    StandardOpenOption.CREATE_NEW,
                    StandardOpenOption.WRITE);
            CommitLogChannel commitLogChannel = new CommitLogChannel(fileChannel);

            UnCheckedConsumer<Integer, IOException> func = i -> {
                String key = "key" + i;
                exp.add(key);
                ByteBuffer bb = ByteBuffer.allocate(4 + key.length());
                bb.putInt(key.length());
                bb.put(key.getBytes());
                commitLogChannel.write(bb.array());
            };
            Time.withTime("Adding", () ->
                    runConcurrentTest(records, func, 50)
            );
        }

        {
            FileChannel fileChannel = FileChannel.open(
                    file.toPath(),
                    StandardOpenOption.SYNC, // DSYNC not always persist data
                    StandardOpenOption.CREATE_NEW,
                    StandardOpenOption.READ);
            for (int i = 0; i < records; i++) {
                ByteBuffer intBB = ByteBuffer.allocate(4);
                fileChannel.read(intBB);
                intBB.position(0);
                int size = intBB.getInt();
                ByteBuffer strBB = ByteBuffer.allocate(size);
                fileChannel.read(strBB);
                String key = new String(strBB.array());
//                System.out.println(key);
                assertTrue(exp.contains(key));
            }
        }


    }

    public static <T extends Exception> void runConcurrentTest(int taskCount,
                                                               UnCheckedConsumer<Integer, T> function,
                                                               int numberOfThreads) throws InterruptedException, ExecutionException, TimeoutException {
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
        if (!executor.awaitTermination(10000, TimeUnit.SECONDS)) {
            throw new InterruptedException();
        }

        assertEquals(taskCount, futures.size());
        for (Future<?> future : futures) {
            future.get(10, TimeUnit.SECONDS);
        }

    }

}