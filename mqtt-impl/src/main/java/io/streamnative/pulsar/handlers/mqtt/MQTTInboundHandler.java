/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.streamnative.pulsar.handlers.mqtt;

import static com.google.common.base.Preconditions.checkArgument;
import static io.streamnative.pulsar.handlers.mqtt.utils.MqttMessageUtils.checkState;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.mqtt.MqttConnectMessage;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttPubAckMessage;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttSubscribeMessage;
import io.netty.handler.codec.mqtt.MqttUnsubscribeMessage;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.streamnative.pulsar.handlers.mqtt.support.DefaultProtocolMethodProcessorImpl;
import io.streamnative.pulsar.handlers.mqtt.support.MQTTCommonConsumer;
import io.streamnative.pulsar.handlers.mqtt.support.MQTTServerCnx;
import io.streamnative.pulsar.handlers.mqtt.utils.NettyUtils;
import io.streamnative.pulsar.handlers.mqtt.utils.PulsarTopicUtils;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.service.Subscription;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;

/**
 * MQTT in bound handler.
 */
@Sharable
@Slf4j
public class MQTTInboundHandler extends ChannelInboundHandlerAdapter {
    private PacketIdGenerator packetIdGenerator = PacketIdGenerator.newNonZeroGenerator();
    static List<MQTTCommonConsumer> commonConsumers = new CopyOnWriteArrayList<>();

    private ProtocolMethodProcessor processor;

    @Getter
    private final MQTTService mqttService;

    public MQTTInboundHandler(MQTTService mqttService) {
        this.mqttService = mqttService;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object message) {
        checkArgument(message instanceof MqttMessage);
        MqttMessage msg = (MqttMessage) message;
        try {
            checkState(msg);
            MqttMessageType messageType = msg.fixedHeader().messageType();
            if (log.isDebugEnabled()) {
                log.debug("Processing MQTT Inbound handler message, type={}", messageType);
            }
            switch (messageType) {
                case CONNECT:
                    checkArgument(msg instanceof MqttConnectMessage);
                    processor.processConnect(ctx.channel(), (MqttConnectMessage) msg);
                    break;
                case SUBSCRIBE:
                    checkArgument(msg instanceof MqttSubscribeMessage);
                    processor.processSubscribe(ctx.channel(), (MqttSubscribeMessage) msg);
                    break;
                case UNSUBSCRIBE:
                    checkArgument(msg instanceof MqttUnsubscribeMessage);
                    processor.processUnSubscribe(ctx.channel(), (MqttUnsubscribeMessage) msg);
                    break;
                case PUBLISH:
                    checkArgument(msg instanceof MqttPublishMessage);
                    processor.processPublish(ctx.channel(), (MqttPublishMessage) msg);
                    break;
                case PUBREC:
                    processor.processPubRec(ctx.channel(), msg);
                    break;
                case PUBCOMP:
                    processor.processPubComp(ctx.channel(), msg);
                    break;
                case PUBREL:
                    processor.processPubRel(ctx.channel(), msg);
                    break;
                case DISCONNECT:
                    processor.processDisconnect(ctx.channel(), msg);
                    break;
                case PUBACK:
                    checkArgument(msg instanceof MqttPubAckMessage);
                    processor.processPubAck(ctx.channel(), (MqttPubAckMessage) msg);
                    break;
                case PINGREQ:
                    processor.processPingReq(ctx.channel());
                    break;
                default:
                    log.error("Unknown MessageType:{}", messageType);
                    break;
            }
        } catch (Throwable ex) {
            log.error("Exception was caught while processing MQTT message, ", ex);
            ctx.close();
        }
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        super.channelActive(ctx);

        synchronized (MQTTInboundHandler.class) {
            if (commonConsumers.size() == 0) {
                log.info("MqttVirtualTopics: Initializing common consumer");
                PulsarService pulsarService = mqttService.getPulsarService();
                MQTTServerConfiguration configuration = mqttService.getServerConfiguration();
                try {
                    String topicName = "c8locals.LocalMqtt";
                    Subscription commonSub = PulsarTopicUtils
                            .getOrCreateSubscription(pulsarService, topicName, "commonSub",
                                    configuration.getDefaultTenant(), configuration.getDefaultNamespace(),
                                    configuration.getDefaultTopicDomain()).get();
                    for (int i = 0; i < 2; i++) {
                        MQTTCommonConsumer consumer = new MQTTCommonConsumer(commonSub, topicName, "common-" + i, new MQTTServerCnx(pulsarService, ctx), packetIdGenerator);
                        commonConsumers.add(consumer);
                        commonSub.addConsumer(consumer);
                        consumer.flowPermits(1000);
                    }
                    log.info("MqttVirtualTopics: Common consumer initialized");
                } catch (InterruptedException | ExecutionException e) {
                    throw new RuntimeException(e);
                }
            } else {
                log.info("MqttVirtualTopics: Reusing common consumer");
            }
        }

        processor = new DefaultProtocolMethodProcessorImpl(mqttService, ctx, commonConsumers);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        processor.processConnectionLost(ctx.channel());
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        log.warn(
                "An unexpected exception was caught while processing MQTT message. "
                + "Closing Netty channel {}. MqttClientId = {}",
                ctx.channel(),
                NettyUtils.getClientId(ctx.channel()),
                cause);
        ctx.close();
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object event) throws Exception {
        if (event instanceof IdleStateEvent) {
            IdleStateEvent e = (IdleStateEvent) event;
            if (e.state() == IdleState.ALL_IDLE) {
                log.warn("close channel : {} due to reached all idle time", NettyUtils.getClientId(ctx.channel()));
                ctx.close();
            }
        } else {
            super.userEventTriggered(ctx, event);
        }
    }

    @Override
    public void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception {
        if (ctx.channel().isWritable()) {
            ctx.channel().flush();
        }
        ctx.fireChannelWritabilityChanged();
    }

}
