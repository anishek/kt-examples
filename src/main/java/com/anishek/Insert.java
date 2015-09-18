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
        for (long i = 0; i < keysToInsert; i++) {
            String asString = String.valueOf(i);
            memcachedClient.add(asString, EXPIRY_30_DAYS, asString);
        }
        return new Result(started.elapsed(TimeUnit.MILLISECONDS));
    }

    static class Result {

        public final long elapsedMillis;

        public Result(long elapsedMillis) {

            this.elapsedMillis = elapsedMillis;
        }
    }
}
