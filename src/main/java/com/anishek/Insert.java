package com.anishek;

import com.google.common.base.Stopwatch;
import net.spy.memcached.MemcachedClient;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

public class Insert implements Callable<Insert.Result> {
    private final MemcachedClient memcachedClient;
    private long keysToInsert;
    private static final int EXPIRY_30_DAYS = 60 * 60 * 24 * 30;

    public Insert(MemcachedClient memcachedClient, long keys) {
        this.memcachedClient = memcachedClient;
        keysToInsert = keys;
    }

    @Override
    public Result call() throws Exception {
        Stopwatch started = Stopwatch.createStarted();
        System.out.println("Start insert from thread : " + Thread.currentThread().getName());
        long passed = 0;
        long failed = 0;
        for (long i = 0; i < keysToInsert; i++) {
            String asString = String.valueOf(i);
            if (memcachedClient.add(asString, EXPIRY_30_DAYS, asString).get()) {
                passed++;
            } else {
                failed++;
            }
        }
        return new Result(passed, failed, started.elapsed(TimeUnit.MILLISECONDS));
    }

    static class Result {

        public final long elapsedMillis;
        public final long passed;
        public final long failed;
        public final String threadName;

        public Result(long passed, long failed, long elapsedMillis) {
            threadName = Thread.currentThread().getName();
            this.elapsedMillis = elapsedMillis;
            this.passed = passed;
            this.failed = failed;
        }
    }
}
