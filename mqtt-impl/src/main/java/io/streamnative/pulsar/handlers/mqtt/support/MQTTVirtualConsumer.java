/**
 * Copyright (c) 2023 Macrometa Corp All rights reserved.
 */
package io.streamnative.pulsar.handlers.mqtt.support;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.streamnative.pulsar.handlers.mqtt.OutstandingPacket;
import io.streamnative.pulsar.handlers.mqtt.OutstandingPacketContainer;
import io.streamnative.pulsar.handlers.mqtt.PacketIdGenerator;
import io.streamnative.pulsar.handlers.mqtt.utils.PulsarTopicUtils;
import lombok.Getter;
import org.apache.bookkeeper.mledger.Entry;

import java.util.Objects;


public class MQTTVirtualConsumer {

    private final String topicName;
    private final MQTTServerCnx cnx;
    private final MqttQoS qos;
    private final PacketIdGenerator packetIdGenerator;
    @Getter
    private final String consumerName;

    public MQTTVirtualConsumer(String topicName, MQTTServerCnx cnx, MqttQoS qos, PacketIdGenerator packetIdGenerator, String consumerName) {
        this.topicName = topicName;
        this.cnx = cnx;
        this.qos = qos;
        this.packetIdGenerator = packetIdGenerator;
        this.consumerName = consumerName;
    }

    public ChannelPromise sendMessage(Entry entry, MqttPublishMessage msg) {
        ChannelPromise promise = cnx.ctx().newPromise();
        cnx.ctx().channel().write(msg);
        cnx.ctx().channel().writeAndFlush(Unpooled.EMPTY_BUFFER, promise);
        return promise;
    }

    public boolean isActive() {
        return !cnx.ctx().isRemoved() && cnx.ctx().channel().isActive();
    }

    @Override
    public boolean equals(Object o) {
        return super.equals(o);
    }


    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), topicName, cnx);
    }

}
