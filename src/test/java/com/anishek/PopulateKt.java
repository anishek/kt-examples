package com.anishek;

import com.google.common.base.Stopwatch;
import com.google.common.util.concurrent.*;
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


    @Test
    public void data() throws IOException, ExecutionException, InterruptedException {
        String[] hostAndPort = System.getProperty("kt.server").split(":");

        InetSocketAddress inetSocketAddress = new InetSocketAddress(hostAndPort[0], Integer.parseInt(hostAndPort[1]));
        int threads = Integer.parseInt(System.getProperty("threads"));
        ListeningExecutorService executors = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(threads + 1, new ThreadFactoryBuilder().setNameFormat("%d").build()));
        long keys = Long.parseLong(System.getProperty("keys"));

        Stopwatch started = Stopwatch.createStarted();
        List<ListenableFuture<Insert.Result>> futures = new ArrayList<>();
        for (int i = 0; i < threads; i++) {
            futures.add(executors.submit(new Insert(Arrays.asList(inetSocketAddress), keys)));
        }
        List<Insert.Result> results = Futures.successfulAsList(futures).get();
        for (Insert.Result result : results) {
            System.out.println(result);
        }
        long elapsed = started.elapsed(TimeUnit.MILLISECONDS);
        System.out.println("Thoughtput : " + (threads * keys * 1000 * 1000) / elapsed);
        System.out.println("Time Taken(millis) : " + elapsed);
    }
}
