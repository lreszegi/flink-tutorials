package com.cloudera.streaming.examples.flink.operators;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ThreadLocalRandom;

public class RandomNumberGenreatorSource implements ParallelSourceFunction<Integer> {

    public static final String SLEEP_MILLIS_KEY = "sleep.millis";
    public static final String SLEEP_NANOS_KEY = "sleep.nanos";
    private static final Logger LOG = LoggerFactory.getLogger(ItemTransactionGeneratorSource.class);
    private final long sleepMillis;
    private final int sleepNanos;
    private volatile boolean isRunning = true;

    public RandomNumberGenreatorSource(ParameterTool params) {
        this.sleepMillis = params.getInt(SLEEP_MILLIS_KEY, 0);
        this.sleepNanos = params.getInt(SLEEP_NANOS_KEY, 0);
    }

    @Override
    public void run(SourceContext<Integer> ctx) throws Exception {
        ThreadLocalRandom rnd = ThreadLocalRandom.current();
        LOG.info("Starting random number generator with throttling {} ms {} ns", sleepMillis, sleepNanos);

        while (this.isRunning) {
            int number = rnd.nextInt(Integer.MAX_VALUE);
            synchronized (ctx.getCheckpointLock()) {
                ctx.collect(number);
            }
            if (sleepMillis > 0) {
                Thread.sleep(sleepMillis);
            }
            if (sleepNanos > 0) {
                final long INTERVAL = sleepNanos;
                long start = System.nanoTime();
                long end;
                do {
                    end = System.nanoTime();
                } while (start + INTERVAL >= end);
            }
        }
    }

    @Override
    public void cancel() {
        this.isRunning = false;
    }
}
