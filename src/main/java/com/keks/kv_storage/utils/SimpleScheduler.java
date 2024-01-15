package com.keks.kv_storage.utils;

import com.keks.kv_storage.server.ThreadUtils;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.*;


public class SimpleScheduler implements Scheduler {

    private final ExecutorService highThreadPool;
    private final ExecutorService normalThreadPool;
    final ScheduledExecutorService scheduledThreadPoolService;

    public SimpleScheduler() {
        this(new Properties());
    }

    public SimpleScheduler(Properties properties) {
//        int priorityThreadPoolSize = Integer.valueOf(properties.getProperty(
//                "scheduler.priorityThreadPoolSize", "5"));
//        int priorityCoreThreads = Integer.valueOf(properties.getProperty(
//                "scheduler.priorityCoreThreads", "2"));
//        int priorityKeepAlive = Integer.valueOf(properties.getProperty(
//                "scheduler.priorityKeepAliveTime", "60"));
//
//        int normalThreadPoolSize = Integer.valueOf(properties.getProperty(
//                "scheduler.normalThreadPoolSize", "5"));
//        int normalCoreThreads = Integer.valueOf(properties.getProperty(
//                "scheduler.normalCoreThreads", "1"));
//        int normalKeepAlive = Integer.valueOf(properties.getProperty(
//                "scheduler.normalKeepAliveTime", "60"));
        // TODO unlimited pool is bad. Better to set limit and log if limit is reached
        highThreadPool = ThreadUtils.createCachedThreadPool("high-priority", false);
        normalThreadPool = ThreadUtils.createCachedThreadPool("normal-priority", false);
        scheduledThreadPoolService = Executors.newSingleThreadScheduledExecutor();
    }


    public ScheduledFuture<?> scheduleWithFixedDelaySecHighPriority(Runnable command,
                                                                    long initialDelay,
                                                                    long delay) {
        return scheduleWithFixedDelay(Priority.HIGH, command, initialDelay, delay, TimeUnit.SECONDS);
    }

    public ScheduledFuture<?> scheduleWithFixedDelaySecNormalPriority(Runnable command,
                                                                      long initialDelay,
                                                                      long delay) {
        return scheduleWithFixedDelay(Priority.NORMAL, command, initialDelay, delay, TimeUnit.SECONDS);
    }


    public ScheduledFuture<?> scheduleWithFixedDelay(Priority priority,
                                                     Runnable command,
                                                     long initialDelay,
                                                     long delay,
                                                     TimeUnit unit) {
        if (priority == Priority.NORMAL) {
            return scheduledThreadPoolService.scheduleAtFixedRate(
                    new ScheduleRunnable(this.normalThreadPool, command),
                    initialDelay,
                    delay,
                    unit);
        } else {
            return scheduledThreadPoolService.scheduleWithFixedDelay(
                    new ScheduleRunnable(highThreadPool, command),
                    initialDelay,
                    delay,
                    unit);
        }
    }

    public void execute(Priority priority, Runnable command) {
        if (priority == Priority.NORMAL)
            normalThreadPool.execute(command);
        else {
            highThreadPool.execute(command);
        }
    }

    public void shutdown() throws InterruptedException {
        scheduledThreadPoolService.shutdownNow();
        scheduledThreadPoolService.awaitTermination(60L, TimeUnit.SECONDS);
        normalThreadPool.shutdownNow();
        normalThreadPool.awaitTermination(60L, TimeUnit.SECONDS);
        highThreadPool.shutdownNow();
        highThreadPool.awaitTermination(60L, TimeUnit.SECONDS);

        Map<Thread, StackTraceElement[]> m = Thread.getAllStackTraces();
        for (Thread t : m.keySet()) {
            StackTraceElement[] se = m.get(t);
            System.err.println(t);
            for (StackTraceElement el : se) {
                System.err.println(el);
            }
        }
    }

    static final class ScheduleRunnable implements Runnable {
        final ExecutorService executor;
        final Runnable runnable;

        ScheduleRunnable(ExecutorService executor, Runnable runnable) {
            this.executor = executor;
            this.runnable = runnable;
        }

        public void run() {
            executor.submit(runnable);
        }

    }
}
