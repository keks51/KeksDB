package com.keks.kv_storage.bplus;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.function.Consumer;


public class TestCalcJavaObjectSize {

    private static final TableName tableName = new TableName("test");

    @Test
    public void test124(@TempDir Path dir) throws IOException, ExecutionException, InterruptedException, TimeoutException {

        CountDownLatch countDownLatch = new CountDownLatch(1);
        countDownLatch.countDown();

        Consumer<Integer> func = i -> {

            try {
                countDownLatch.await();
//                System.out.println(i);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }


        };


    }

    public static void runConcurrentTest(int taskCount,
                                         int threadPoolAwaitTimeoutSec,
                                         int taskAwaitTimeoutSec,
                                         Consumer<Integer> function,
                                         int numberOfThreads) throws InterruptedException, ExecutionException, TimeoutException {
        ExecutorService executor = Executors.newFixedThreadPool(numberOfThreads);
        List<Future<?>> futures = new ArrayList<>(taskCount);
        for (int i = 0; i < taskCount; i++) {
            int y = i;
            Future<?> future1 = executor.submit(() -> function.accept(y));
            futures.add(future1);
        }

        executor.shutdown();
        if (!executor.awaitTermination(threadPoolAwaitTimeoutSec, TimeUnit.SECONDS)) {
            throw new InterruptedException();
        }

        for (Future<?> future : futures) {
            future.get(taskAwaitTimeoutSec, TimeUnit.SECONDS);
//            future.get();
        }

    }


}
