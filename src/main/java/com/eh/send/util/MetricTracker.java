package com.eh.send.util;

import java.util.concurrent.atomic.AtomicInteger;

public final class MetricTracker {
    private final int inputRatePerSecond;
    private final int durationInSeconds;
    private final AtomicInteger recordNumber;
    private final AtomicInteger completionCounter;

    public MetricTracker(int inputRatePerSecond, int durationInSeconds) {
        this.inputRatePerSecond = inputRatePerSecond;
        this.durationInSeconds = durationInSeconds;
        this.recordNumber = new AtomicInteger(0);
        this.completionCounter = new AtomicInteger(0);
    }

    public int getNextRecord() {
        return this.recordNumber.getAndIncrement();
    }

    public void incrementCompletionCount() {
        this.completionCounter.incrementAndGet();
    }

    public void printSnapShot() {
        long kUsed = (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) / 1024;
        long megUsed = kUsed / 1024;
        System.out.println("After " + this.recordNumber + " records, memory used = " + megUsed + "MB");
    }

    public void printFinal() {
        System.out.printf("Input rate: %d records/s. Pushed: %d records/s Completed: [%d records/s, total %d] %n",
                this.inputRatePerSecond,
                this.recordNumber.get() / this.durationInSeconds,
                this.completionCounter.get() / this.durationInSeconds,
                this.completionCounter.get());
    }
}
