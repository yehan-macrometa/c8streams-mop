package io.streamnative.pulsar.handlers.mqtt.support;

import io.streamnative.pulsar.handlers.mqtt.MQTTProtocolHandler;
import io.streamnative.pulsar.handlers.mqtt.MQTTServerConfiguration;
import io.streamnative.pulsar.handlers.mqtt.utils.PulsarTopicUtils;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class MQTTPublisherContext {
    private static MQTTPublisherContext instance;
    private final PulsarClient client;
    private final Map<String, CompletableFuture<Producer<byte[]>>> producers = new ConcurrentHashMap<>();

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

            initProducers(brokerService, serverConfiguration);
        } catch (PulsarClientException e) {
            throw new RuntimeException(e);
        }
    }

    private void initProducers(BrokerService brokerService, MQTTServerConfiguration serverConfiguration) {
        List<String> topics = MQTTProtocolHandler.getRealTopics(serverConfiguration.getMqttRealTopicNamePrefix(),
                serverConfiguration.getMqttRealTopicCount());
        if (topics == null) {
            return;
        }
        topics.forEach(topic -> {
            try {
                Optional<Boolean> redirect = PulsarTopicUtils.isTopicRedirect(brokerService.getPulsar(), topic, serverConfiguration.getDefaultTenant(), serverConfiguration.getDefaultNamespace(), true
                        , serverConfiguration.getDefaultTopicDomain()).get();
                if (!redirect.orElse(true)) {
                    getProducer(topic).get();
                }
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
        });
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

    public CompletableFuture<Producer<byte[]>> getProducer(String topic) {
        return producers.computeIfAbsent(topic, t -> client.newProducer()
                .topic(t)
                .blockIfQueueFull(true)
                .sendTimeout(1, TimeUnit.MINUTES)
                .maxPendingMessages(100_000)
                .batchingMaxMessages(1000)
                .batchingMaxPublishDelay(10, TimeUnit.MILLISECONDS)
                .createAsync());
    }
}
