package com.anishek;

import com.google.common.base.Stopwatch;
import net.spy.memcached.MemcachedClient;
import net.spy.memcached.internal.OperationFuture;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class Insert implements Callable<Insert.Result> {
    private final MemcachedClient memcachedClient;
    private long keysToInsert;
    private static final int EXPIRY_30_DAYS = 60 * 60 * 24 * 30;

    public Insert(List<InetSocketAddress> socketAddresses, long keys) throws IOException {
        this.memcachedClient = new MemcachedClient(socketAddresses);
        keysToInsert = keys;
    }

    @Override
    public Result call() throws Exception {
        Stopwatch started = Stopwatch.createStarted();
        System.out.println("Start insert from thread : " + Thread.currentThread().getName());
        long passed = 0, failed = 0, otherException = 0, timeout = 0;
        long start = Long.parseLong(Thread.currentThread().getName());
        for (long i = (start * keysToInsert); i < (start + 1) * keysToInsert; i++) {
            String asString = String.valueOf(i);
            OperationFuture<Boolean> add = memcachedClient.add(asString, EXPIRY_30_DAYS, asString);

            try {
                Boolean result = add.get(1, TimeUnit.SECONDS);
                if (result) {
                    passed++;
                } else {
                    failed++;
                }
            } catch (TimeoutException e) {
                timeout++;
            } catch (Exception e) {
                otherException++;
            }
        }
        memcachedClient.shutdown();
        return new Result(passed, failed, timeout, otherException, started.elapsed(TimeUnit.MILLISECONDS));
    }

    static class Result {

        public final long elapsedMillis;
        public final long passed;
        public final long failed;
        public final String threadName;
        public final long timeout;
        public final long otherException;

        public Result(long passed, long failed, long timeout, long otherException, long elapsedMillis) {
            this.timeout = timeout;
            this.otherException = otherException;
            threadName = Thread.currentThread().getName();
            this.elapsedMillis = elapsedMillis;
            this.passed = passed;
            this.failed = failed;
        }

        @Override
        public String toString() {
            return "thread Name: " + threadName + "\n"
                    + "passed: " + passed + "\n"
                    + "failed: " + failed + "\n"
                    + "timeout: " + timeout + "\n"
                    + "other Exception: " + otherException + "\n";
        }
    }
}
