package com.anishek;

import com.google.common.base.Stopwatch;
import net.spy.memcached.MemcachedClient;
import net.spy.memcached.internal.CheckedOperationTimeoutException;
import net.spy.memcached.internal.OperationFuture;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class InsertWithTimeout implements Insert {
    private final MemcachedClient memcachedClient;
    private long keysToInsert;
    private static final int EXPIRY_30_DAYS = 60 * 60 * 24 * 30;
    private final PostOpFunction function;

    public InsertWithTimeout(List<InetSocketAddress> socketAddresses, long keys) throws IOException {
        this.memcachedClient = new MemcachedClient(socketAddresses);
        keysToInsert = keys;
        this.function = new PostOpFunction() {
            @Override
            public void doOperation() {
                memcachedClient.shutdown();
            }
        };
    }

    public InsertWithTimeout(MemcachedClient memcachedClient, long keys) {
        this.memcachedClient = memcachedClient;
        this.keysToInsert = keys;
        this.function = new PostOpFunction() {
            @Override
            public void doOperation() {
                //do nothing
            }
        };
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
            } catch (CheckedOperationTimeoutException e) {
                timeout++;
            } catch (Exception e) {
                otherException++;
            }
        }
        function.doOperation();
        return new Result(passed, failed, timeout, otherException, started.elapsed(TimeUnit.MILLISECONDS));
    }

}
