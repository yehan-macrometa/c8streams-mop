/**
 * Copyright (c) 2023 Macrometa Corp All rights reserved.
 */
package io.streamnative.pulsar.handlers.mqtt.support;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.streamnative.pulsar.handlers.mqtt.OutstandingPacket;
import io.streamnative.pulsar.handlers.mqtt.OutstandingVirtualPacket;
import io.streamnative.pulsar.handlers.mqtt.OutstandingVirtualPacketContainer;
import io.streamnative.pulsar.handlers.mqtt.PacketIdGenerator;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.Entry;
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

    public MQTTVirtualConsumer(String topicName, MQTTServerCnx cnx, MqttQoS qos, PacketIdGenerator packetIdGenerator, String consumerName, OutstandingVirtualPacketContainer outstandingVirtualPacketContainer) {
        this.topicName = topicName;
        this.cnx = cnx;
        this.qos = qos;
        this.packetIdGenerator = packetIdGenerator;
        this.consumerName = consumerName;
        this.outstandingVirtualPacketContainer = outstandingVirtualPacketContainer;
    }

    /*public ChannelPromise sendMessage(Entry entry, MqttPublishMessage msg, int packetId) {
        if (MqttQoS.AT_MOST_ONCE != qos) {
            outstandingVirtualPacketContainer.add(new OutstandingVirtualPacket(this, packetId, entry.getLedgerId(),
                    entry.getEntryId()));
        }

        ChannelPromise promise = cnx.ctx().newPromise();
        cnx.ctx().channel().write(msg);
        cnx.ctx().channel().writeAndFlush(Unpooled.EMPTY_BUFFER, promise);
        return promise;
    }*/

    public ChannelPromise sendMessage(MqttPublishMessage msg, int packetId, MessageId messageId) {
        if (MqttQoS.AT_MOST_ONCE != qos) {
            outstandingVirtualPacketContainer.add(new OutstandingVirtualPacket(this, packetId, messageId));
        }

        ChannelPromise promise = cnx.ctx().newPromise();
        cnx.ctx().channel().write(msg);
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

