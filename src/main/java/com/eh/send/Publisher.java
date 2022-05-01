package com.eh.send;

import com.azure.messaging.eventhubs.EventData;
import com.azure.messaging.eventhubs.EventHubClientBuilder;
import com.azure.messaging.eventhubs.EventHubProducerAsyncClient;
import reactor.core.publisher.Mono;

import java.util.concurrent.CompletableFuture;

final class Publisher implements AutoCloseable {
    private final EventHubProducerAsyncClient producer;

    Publisher() {
        this.producer = new EventHubClientBuilder()
                .connectionString(System.getenv("EH_CON_STR"), System.getenv("EH_NAME"))
                .buildAsyncProducerClient();
    }

    public CompletableFuture<Void> publish(byte[] data) {
        Mono<Void> mono = producer.createBatch().flatMap(batch -> {
            batch.tryAdd(new EventData(data));
            return producer.send(batch);
        });
        return mono.toFuture();
    }

    @Override
    public void close() throws Exception {
        this.producer.close();
    }
}
