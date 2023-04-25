package io.streamnative.pulsar.handlers.mqtt.support;

import io.streamnative.pulsar.handlers.mqtt.MQTTServerConfiguration;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

@Slf4j
public class MQTTPublisherContext {
    private static MQTTPublisherContext instance;
    private final PulsarClient client;
    private static final Map<String, Producer<byte[]>> producers = new ConcurrentHashMap<>();

    private MQTTPublisherContext(BrokerService brokerService, MQTTServerConfiguration serverConfiguration) {
        int numThreads = serverConfiguration.getMqttNumConsumerThreads();

        try {
            client = PulsarClient.builder()
                    .serviceUrl("pulsar://localhost:6650")
                    .authentication(
                            brokerService.getPulsar().getConfiguration().getBrokerClientAuthenticationPlugin(),
                            brokerService.getPulsar().getConfiguration().getBrokerClientAuthenticationParameters())
                    .operationTimeout(1, TimeUnit.MINUTES)
                    .connectionsPerBroker(numThreads)
                    .ioThreads(numThreads)
                    .listenerThreads(numThreads)
                    .build();
        } catch (PulsarClientException e) {
            throw new RuntimeException(e);
        }
    }

    public static void init(BrokerService brokerService, MQTTServerConfiguration serverConfiguration) {
        if (instance == null) {
            instance = new MQTTPublisherContext(brokerService, serverConfiguration);
        } else {
            throw new IllegalStateException("MQTTPublisherContext is already initialized.");
        }
    }

    public static MQTTPublisherContext getInstance() {
        if (instance != null) {
            return instance;
        } else {
            throw new IllegalStateException("MQTTPublisherContext is not initialized yet.");
        }
    }

    public static CompletableFuture<MessageId> publishMessages(Message<byte[]> message, String topic) {
        return getInstance().getProducer(topic).thenCompose(producer -> producer.newMessage()
                .properties(message.getProperties())
                .value(message.getValue())
                .sendAsync());
    }

    public synchronized CompletableFuture<Producer<byte[]>> getProducer(String topic) {
        CompletableFuture<Producer<byte[]>> future = new CompletableFuture<>();
        Producer<byte[]> producer = producers.get(topic);
        if (producer != null) {
            future.complete(producer);
        } else {
            synchronized (this) {
                producer = producers.get(topic);
                if (producer != null) {
                    future.complete(producer);
                } else {
                    try {
                        producer = client.newProducer()
                            .topic(topic)
                            .blockIfQueueFull(true)
                            .sendTimeout(1, TimeUnit.MINUTES)
                            .maxPendingMessages(100_000)
                            .batchingMaxMessages(1000)
                            .batchingMaxPublishDelay(10, TimeUnit.MILLISECONDS)
                            .create();
                        producers.put(topic, producer);
                        future.complete(producer);
                        log.info("Created Producer for pulsar topic = " + topic);
                    } catch (PulsarClientException e) {
                        future.completeExceptionally(e);
                    }
                }
            }
        }
        return future;
    }
}
