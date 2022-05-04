package com.eh.send;

import com.azure.messaging.eventhubs.*;
import com.azure.messaging.eventhubs.models.EventPosition;
import com.azure.messaging.eventhubs.models.PartitionEvent;
import com.azure.messaging.eventhubs.models.SendOptions;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

public final class ProfilePublishReceive implements Closeable {
    private final EventHubProducerAsyncClient producer;
    private final EventHubConsumerAsyncClient receiver;
    private final List<Long> latencies = new ArrayList<>(1000000); // an array to record latencies

    public static void main(String[] args) throws IOException {
        ProfilePublishReceive app = new ProfilePublishReceive();

        final int warmupMessages = 10;   // The number of messages to "publish and receive" during warmup period.
        final int durationInSec = 600;   // The total duration to "publish and receive" (includes warmup and measurement).
        final String partitionId = "2"; // The "empty" partition to send and receive.

        try {
            app.warmupAndMeasure(warmupMessages, durationInSec, partitionId);
        } finally {
            app.close();
        }
    }

    // Ctr
    public ProfilePublishReceive() {
        EventHubClientBuilder builder = new EventHubClientBuilder()
                .connectionString(System.getenv("EH_CON_STR"), System.getenv("EH_NAME"))
                .consumerGroup("$Default");

        this.producer = builder
                .buildAsyncProducerClient();

        this.receiver = builder
                .buildAsyncConsumerClient();
    }

    // First warmup the publisher and receiver by transmitting {@code warmupMessages} messages then
    // measure for a maximum duration of {@code durationInSec}.
    public void warmupAndMeasure(int warmupMessages, int durationInSec, String partitionId) {
        // Obtain the partition receiver flux.
        Flux<PartitionEvent> recvFlux = receiver.receiveFromPartition(partitionId, EventPosition.latest());

        Flux<Tuple2<SimpleEntry<String, Long>, SimpleEntry<String, Long>>> publishReceive =
                Mono.fromSupplier(() -> {
                    // create a unique message id.
                    return UUID.randomUUID().toString();
                })
                .repeat() // keep flowing the message ids
                .concatMap(sendMessageId -> {
                    final Long sendTime = System.currentTimeMillis();
                    // send a message with the message id.
                    final EventData message = new EventData(sendMessageId).setMessageId(sendMessageId);
                    return this.producer.send(
                                    Collections.singletonList(message),
                                    new SendOptions().setPartitionId(partitionId))
                            .thenReturn(new SimpleEntry<>(sendMessageId, sendTime));
                }, 1)
                .zipWith(recvFlux.map(message -> {
                    // receives a message
                    final String recvMessageId = message.getData().getMessageId();
                    final Long recvTime = System.currentTimeMillis();

                    return new SimpleEntry<>(recvMessageId, recvTime);
                }), 1)
                .doOnNext(entry -> {
                    final String sendMessageId = entry.getT1().getKey();
                    final String recvMessageId = entry.getT2().getKey();
                    // assert we received what we sent
                    if (!sendMessageId.equalsIgnoreCase(recvMessageId)) {
                        throw new IllegalStateException("Mismatched send and receive message ids");
                    }

                    final long sendTime = entry.getT1().getValue();
                    final long recvTime = entry.getT2().getValue();
                    final long latency = recvTime - sendTime;
                    latencies.add(latency);
                });

        final boolean startedMeasurement[] = new boolean[1];
        startedMeasurement[0] = false;

        final long measureStart[] = new long[1];
        final long measureEnd[] = new long[1];

        publishReceive
                .skip(warmupMessages) // Warm-up: send-receive N messages.
                .doOnNext(ignored ->{
                    if (!startedMeasurement[0]) {
                        System.out.println("Warmup completed.. starting measurement");
                        startedMeasurement[0] = true;
                        latencies.clear();
                        measureStart[0] = System.currentTimeMillis();
                    }
                })
                .take(Duration.ofSeconds(durationInSec))
                .blockLast();

        measureEnd[0] = System.currentTimeMillis();

        System.out.printf("Measurement completed. duration: %dms \n", measureEnd[0] - measureStart[0]);
        printMeasurement();
    }

    public void printMeasurement() {
        Collections.sort(latencies);
        final int size = latencies.size();

        int offset_99 = size / 100;
        offset_99 = offset_99 == 0 ? 1 : offset_99;

        int offset_99_9 = size / 1000;
        offset_99_9 = offset_99_9 == 0 ? 1 : offset_99_9;

        int offset_99_99 = size / 10000;
        offset_99_99 = offset_99_99 == 0 ? 1 : offset_99_99;

        System.out.printf("Completed %d send-receive messages \n", size);
        System.out.printf(
                " 50    percentile %d ms\n" +
                " 75    percentile %d ms\n" +
                " 90    percentile %d ms\n" +
                " 99    percentile %d ms\n" +
                " 99.9  percentile %d ms\n" +
                " 99.99 percentile %d ms\n" +
                " 100   percentile %d ms\n",
                latencies.get(size / 2),
                latencies.get(size * 7 / 10),
                latencies.get(size * 9 / 10),
                latencies.get(size - offset_99),
                latencies.get(size - offset_99_9),
                latencies.get(size - offset_99_99),
                latencies.get(size - 1));
    }

    @Override
    public void close() throws IOException {
        this.producer.close();
        this.receiver.close();
    }
}
