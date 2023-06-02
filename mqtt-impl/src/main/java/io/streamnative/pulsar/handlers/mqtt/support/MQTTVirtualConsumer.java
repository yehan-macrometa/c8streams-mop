/**
 * Copyright (c) 2023 Macrometa Corp All rights reserved.
 */
package io.streamnative.pulsar.handlers.mqtt.support;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.streamnative.pulsar.handlers.mqtt.Connection;
import io.streamnative.pulsar.handlers.mqtt.OutstandingPacket;
import io.streamnative.pulsar.handlers.mqtt.OutstandingVirtualPacket;
import io.streamnative.pulsar.handlers.mqtt.OutstandingVirtualPacketContainer;
import io.streamnative.pulsar.handlers.mqtt.PacketIdGenerator;
import io.streamnative.pulsar.handlers.mqtt.adapter.MqttAdapterMessage;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.MessageId;

import java.nio.charset.StandardCharsets;
import java.util.Objects;

@Slf4j
public class MQTTVirtualConsumer {
    
    @Getter
    private final String topicName;
    @Getter
    private final MQTTServerCnx cnx;
    @Getter
    private final MqttQoS qos;
    private final PacketIdGenerator packetIdGenerator;
    @Getter
    private final String consumerName;
    private final OutstandingVirtualPacketContainer outstandingVirtualPacketContainer;
    @Getter
    private final Connection connection;
    
    public MQTTVirtualConsumer(String topicName, Connection connection, MQTTServerCnx cnx,
                               MqttQoS qos, PacketIdGenerator packetIdGenerator, String consumerName,
                               OutstandingVirtualPacketContainer outstandingVirtualPacketContainer) {
        this.topicName = topicName;
        this.cnx = cnx;
        this.qos = qos;
        this.packetIdGenerator = packetIdGenerator;
        this.consumerName = consumerName;
        this.outstandingVirtualPacketContainer = outstandingVirtualPacketContainer;
        this.connection = connection;
    }
    
    public ChannelPromise sendMessage(MqttPublishMessage msg, Consumer<byte[]> pulsarConsumer, int packetId, MessageId messageId) {
        if (MqttQoS.AT_MOST_ONCE != qos) {
            outstandingVirtualPacketContainer.add(new OutstandingVirtualPacket(pulsarConsumer, packetId, messageId));
        }
        
        ChannelPromise promise = cnx.ctx().newPromise();
        cnx.ctx().channel().write(new MqttAdapterMessage(connection.getClientId(), msg,
            connection.isFromProxy()));
        cnx.ctx().channel().writeAndFlush(Unpooled.EMPTY_BUFFER, promise);
        
        return promise;
    }
    
    @Override
    public boolean equals(Object o) {
        return super.equals(o);
    }
    
    
    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), topicName, cnx);
    }
    
    public void close() {
        cnx.ctx().channel().close();
    }
    
}