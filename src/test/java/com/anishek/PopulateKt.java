package com.anishek;

import com.google.common.base.Stopwatch;
import com.google.common.util.concurrent.*;
import net.spy.memcached.MemcachedClient;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * /usr/local/kyototycoon/bin/ktserver -cmd /data/kt/scripts -dmn -pid /var/lib/kyoto/anishek_kt.pid -log /var/log/kyoto/anishek_kt.log -ls -ulog /home/anishek/kyoto/0001-ulog -ulim 1024m -uasi 600 -th 6 -plsv /usr/local/kyototycoon/libexec/ktplugservmemc.so -plex port=11212#opts=f -sid 1 /home/anishek/kyoto/db/anishek.kch#bnum=500000000#msiz=12g#dfunit=8
 */
public class PopulateKt {

    interface ObjectCreator {
        Insert instance(long keys) throws IOException;
    }

    @Test
    public void insertWait() throws IOException, ExecutionException, InterruptedException {
        String[] hostAndPort = System.getProperty("kt.server").split(":");
        final InetSocketAddress inetSocketAddress = new InetSocketAddress(hostAndPort[0], Integer.parseInt(hostAndPort[1]));
        ObjectCreator objectCreator = new ObjectCreator() {
            @Override
            public Insert instance(long keys) throws IOException {
                return new InsertWithTimeout(Arrays.asList(inetSocketAddress), keys);
            }
        };
        run(objectCreator);
    }

    @Test
    public void insertWaitSingleClient() throws IOException, ExecutionException, InterruptedException {
        String[] hostAndPort = System.getProperty("kt.server").split(":");
        InetSocketAddress inetSocketAddress = new InetSocketAddress(hostAndPort[0], Integer.parseInt(hostAndPort[1]));
        final MemcachedClient client = new MemcachedClient(inetSocketAddress);

        ObjectCreator objectCreator = new ObjectCreator() {
            @Override
            public Insert instance(long keys) throws IOException {
                return new InsertWithTimeout(client, keys);
            }

        };
        run(objectCreator);
        client.shutdown();
    }

    @Test
    public void insertNoWait() throws InterruptedException, ExecutionException, IOException {
        String[] hostAndPort = System.getProperty("kt.server").split(":");
        final InetSocketAddress inetSocketAddress = new InetSocketAddress(hostAndPort[0], Integer.parseInt(hostAndPort[1]));
        ObjectCreator objectCreator = new ObjectCreator() {
            @Override
            public Insert instance(long keys) throws IOException {
                return new InsertWithNoTimeout(Arrays.asList(inetSocketAddress), keys);

            }
        };
        run(objectCreator);
    }

    private void run(ObjectCreator objectCreator) throws IOException, InterruptedException, ExecutionException {
        int threads = Integer.parseInt(System.getProperty("threads"));
        ListeningExecutorService executors = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(threads + 1, new ThreadFactoryBuilder().setNameFormat("%d").build()));
        long keys = Long.parseLong(System.getProperty("keys"));

        Stopwatch started = Stopwatch.createStarted();
        List<ListenableFuture<Result>> futures = new ArrayList<>();
        for (int i = 0; i < threads; i++) {
            futures.add(executors.submit(objectCreator.instance(keys)));
        }
        List<Result> results = Futures.successfulAsList(futures).get();
        for (Result result : results) {
            System.out.println(result);
        }
        long elapsedInMillis = started.elapsed(TimeUnit.MILLISECONDS);
        System.out.println("Thoughtput : " + (threads * keys * 1000) / elapsedInMillis);
        System.out.println("Time Taken(millis) : " + elapsedInMillis);
    }

    @Test
    public void read() throws IOException {
        String[] hostAndPort = System.getProperty("kt.server").split(":");

        InetSocketAddress inetSocketAddress = new InetSocketAddress(hostAndPort[0], Integer.parseInt(hostAndPort[1]));
        MemcachedClient memcachedClient = new MemcachedClient(inetSocketAddress);
        long start = Long.parseLong(System.getProperty("start.key"));
        long end = Long.parseLong(System.getProperty("end.key"));

        for (long i = start; i < end; i++) {
            try {
                Long.parseLong(memcachedClient.get(String.valueOf(i)).toString());
            } catch (Exception e) {
                System.out.println("failed for key :" + i);
                break;
            }
        }
        memcachedClient.shutdown();
    }
}
