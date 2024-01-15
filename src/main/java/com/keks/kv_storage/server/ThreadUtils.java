package com.keks.kv_storage.server;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;


public class ThreadUtils {

    public static ExecutorService createCachedThreadPool(String poolName, boolean setDaemon) {
        ThreadFactory threadFactory = new ThreadFactory() {
            final AtomicLong count = new AtomicLong();
            @Override
            public Thread newThread(Runnable r) {
                Thread thread = new Thread(r);
                thread.setDaemon(setDaemon);
                thread.setName(poolName + " Thread: " + Thread.currentThread().getId() + " Worker-" + count.getAndIncrement());
                return thread;
            }
        };
        return Executors.newCachedThreadPool(threadFactory);
    }

    public static ExecutorService createLimitedExecutorService(String poolName, int minThreads, int maxThreads, boolean setDaemon) {
//        RejectedExecutionHandler handler = new MyHandler();
        RejectedExecutionHandler handler = new ThreadPoolExecutor.AbortPolicy();
        return new ThreadPoolExecutor(
                minThreads,
                maxThreads,
                60L,
                TimeUnit.SECONDS,
                new SynchronousQueue<>(),
                new ThreadFactory() {
                    final AtomicLong count = new AtomicLong();

                    @Override
                    public Thread newThread(Runnable r) {
                        Thread thread = new Thread(r);
                        thread.setDaemon(setDaemon);
                        thread.setName(poolName + " Thread: " + Thread.currentThread().getId() + " Worker-" + count.getAndIncrement());
                        return thread;
                    }
                },
                handler
        );
    }

    static class MyHandler implements RejectedExecutionHandler {

        @Override
        public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
            System.out.println("here123");
            Field[] declaredFields = r.getClass().getDeclaredFields();
            Method[] declaredMethods = r.getClass().getDeclaredMethods();

            try {
                Field rawoutField = r.getClass().getDeclaredField("tx");
                rawoutField.setAccessible(true);
                Object o = rawoutField.get(r);
                System.out.println();
//                Field rawoutField = r.getClass().getDeclaredField("rawout");
//                rawoutField.setAccessible(true);
//                Field connectionField = r.getClass().getDeclaredField("connection");
//                connectionField.setAccessible(true);
//
//                Field field = connectionField.getType().getDeclaredField("rawout");
//                field.setAccessible(true);
//                Object o = field.get(connectionField.get(r));
//                rawoutField.set(r, field);
//                Method declaredMethod = r.getClass().getDeclaredMethod("reject", int.class, String.class, String.class);
////                declaredMethods[1].setAccessible(true);
////                declaredMethods[1].invoke(r, 429, "1", "2");
//                declaredMethod.setAccessible(true);
//                declaredMethod.invoke(r, 429, "1", "2");
            } catch (Throwable e) {
                throw new RuntimeException(e);
            }
            throw new RuntimeException();
        }
    }

}
