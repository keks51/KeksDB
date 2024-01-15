package com.keks.kv_storage.bplus.page_manager;

import com.keks.kv_storage.bplus.conf.BPlusConf;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.function.Consumer;


class BplusTreeRuntimeParametersTest {

    @Test
    public void test1(@TempDir Path dir) throws IOException, ExecutionException, InterruptedException, TimeoutException {

        BPlusConf btreeParams = new BPlusConf(4, 10, 10, 40_000_000);
        BplusTreeRuntimeParameters bplusTreeRuntimeParameters = new BplusTreeRuntimeParameters(btreeParams.btreeConf.treeOrder, dir.toFile());

        int times = 100_000;
        Consumer<Integer> func = i -> {
            bplusTreeRuntimeParameters.incTreeHeight();
        };
        runConcurrentTest(times, 1000, 1, func, 200);

        assert(times == bplusTreeRuntimeParameters.getTreeHeight());
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