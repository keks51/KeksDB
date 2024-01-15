package com.keks.kv_storage.utils;

import io.micrometer.core.instrument.Timer;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;


public class Time {

    public static volatile boolean mes = false;

    public static <R extends Exception> void withTimeChecked(String msg, UnCheckedRunnable<R> f) throws R {
        Instant start = Instant.now();
        try {
            f.run();
        } finally {
            Instant finish = Instant.now();
            Duration between = Duration.between(start, finish);
            System.out.println(msg + " : " + between);
        }
    }

    public static <EX extends Exception> void withTime(String msg, UnCheckedRunnable<EX> f) throws EX {
        Instant start = Instant.now();
        try {
            f.run();
        } finally {
            Instant finish = Instant.now();
            Duration between = Duration.between(start, finish);
            System.out.println(msg + " : " + between);
        }
    }

    public static <EX extends Exception> void withTimer(Timer timer, UnCheckedRunnable<EX> f) throws EX {
        if (mes) {
            Instant start = Instant.now();
            try {
                f.run();
            } finally {
                Instant finish = Instant.now();
                Duration between = Duration.between(start, finish);

                timer.record(between.toNanos(), TimeUnit.NANOSECONDS);
            }
        } else {
            f.run();
        }
    }

    public static <EX extends Exception> void withTimerMillis(Timer timer, UnCheckedRunnable<EX> f) throws EX {
        if (mes) {
            Instant start = Instant.now();
            try {
                f.run();
            } finally {
                Instant finish = Instant.now();
                Duration between = Duration.between(start, finish);

                timer.record(between.toMillis(), TimeUnit.MILLISECONDS);
            }
        } else {
            f.run();
        }
    }

    public static <EX extends Exception, T> T withTimer(Timer timer, UnCheckedSupplier<T, EX> f) throws EX {
        if (mes) {
            Instant start = Instant.now();
            try {
                return f.get();
            } finally {
                Instant finish = Instant.now();
                Duration between = Duration.between(start, finish);
//            System.out.println(msg + " : " + between);
                timer.record(between.toNanos(), TimeUnit.NANOSECONDS);
            }
        } else {
            return f.get();
        }

    }

    public static <T, EX extends Exception> T withTimeChecked(String msg, UnCheckedSupplier<T, EX> f) throws EX {
        Instant start = Instant.now();
        try {
            return f.get();
        } finally {
            Instant finish = Instant.now();
            Duration between = Duration.between(start, finish);
            System.out.println(msg + " : " + between);
        }
    }

    public static <T> T withTime(String msg, Supplier<T> f) {
        Instant start = Instant.now();
        try {
            return f.get();
        } finally {
            Instant finish = Instant.now();
            Duration between = Duration.between(start, finish);
            System.out.println(msg + " : " + between);
        }
    }

}
