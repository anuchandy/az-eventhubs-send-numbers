package com.eh.send;

import com.eh.send.util.MetricTracker;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

public class ProfilePublishing {
    private static final String usage = "Provide 3 params in the order: recordsPerSeconds durationSeconds f|c";

    public ProfilePublishing() {
    }

    public static void main(String[] args) {
        if (args.length < 3) {
            throw new IllegalArgumentException(usage);
        } else {
            // argument indicating the number of events to publish per second.
            // argument indicating how long in total the publish operation should run.
            final int recordsPerSecond = Integer.parseInt(args[0]);
            final int durationSeconds = Integer.parseInt(args[1]);

            // argument indicating the mode of publish - 'f' for fire&forget, 'c' for coordinated.
            // Note: fire&forget means - send N events per second, such that don't wait for the last N events pushed to
            //                           complete before sending another N in the next second.
            //       coordinated means - send N events in a second, but before commencing another N events in next second,
            //                           block for previous batch complete. this means subsequent sending may start late.
            final String fORc = args[2];

            if (fORc == null || (!"f".equalsIgnoreCase(fORc) && !"c".equalsIgnoreCase(fORc))) {
                throw new IllegalArgumentException(usage);
            }

            final boolean isFireAndForget = "f".equalsIgnoreCase(fORc);

            Publisher publisher = new Publisher();

            try {
                new ProfilePublishing().run(publisher, recordsPerSecond, durationSeconds, isFireAndForget);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private void run(Publisher publisher,
                     int recordsPerSecond,
                     int durationSeconds,
                     boolean fireAndForget) throws Exception {

        System.out.println("(Starting Run) recordsPerSecond: " + recordsPerSecond
                + " durationSeconds: " + durationSeconds
                + " FireAndForget: " + fireAndForget
                + "\n");

        final MetricTracker metric = new MetricTracker(recordsPerSecond, durationSeconds);

        ScheduledExecutorService doThisOncePerSec = Executors.newScheduledThreadPool(1);
        doThisOncePerSec.scheduleAtFixedRate(() -> {
            List<CompletableFuture<?>> publishCompletableFutures = new ArrayList<>();

            // Starts 'recordsPerSecond' number of Publish Async Work.
            for (int i = 0; i < recordsPerSecond; i++) {
                final byte[] body = ("msg#" + metric.getNextRecord()).getBytes(StandardCharsets.UTF_8);

                if (fireAndForget) {
                    // The fire&forget-mode doesn't track the async-completion, so ignore return value.
                    publisher.publish(body)
                            .thenAccept(ignored -> metric.incrementCompletionCount());
                } else {
                    // The coordinated-mode needs to track the async-completion.
                    final CompletableFuture<Void> future = publisher.publish(body)
                            .thenAccept(ignored -> metric.incrementCompletionCount());
                    publishCompletableFutures.add(future);
                }
            }

            if (!fireAndForget) {
                try {
                    // Let's merge the async-completion in coordinated-mode...
                    final CompletableFuture<Void> allFutures
                            = CompletableFuture.allOf(publishCompletableFutures.toArray(CompletableFuture[]::new));
                    // ...and wait for all to complete.
                    allFutures.get();
                } catch (InterruptedException | ExecutionException e) {
                    System.err.println("Error occurred while waiting for the result. " + e);
                }
            }
            metric.printSnapShot();

        }, 1, 1, TimeUnit.SECONDS);

        try {
            doThisOncePerSec.awaitTermination(durationSeconds, TimeUnit.SECONDS);
            doThisOncePerSec.shutdown();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            publisher.close();
        }

        metric.printFinal();
        System.out.println("All done.");
    }
}
