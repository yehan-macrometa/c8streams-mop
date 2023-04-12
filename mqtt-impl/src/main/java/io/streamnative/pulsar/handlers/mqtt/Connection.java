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

import static io.streamnative.pulsar.handlers.mqtt.Connection.ConnectionState.CONNECT_ACK;
import static io.streamnative.pulsar.handlers.mqtt.Connection.ConnectionState.DISCONNECTED;
import static io.streamnative.pulsar.handlers.mqtt.Connection.ConnectionState.ESTABLISHED;
import static java.util.concurrent.atomic.AtomicReferenceFieldUpdater.newUpdater;
import io.netty.channel.Channel;
import io.netty.handler.codec.mqtt.MqttConnAckMessage;
import io.netty.handler.codec.mqtt.MqttConnectReturnCode;
import io.streamnative.pulsar.handlers.mqtt.support.MQTTCommonConsumer;
import io.streamnative.pulsar.handlers.mqtt.support.MQTTVirtualConsumer;
import io.streamnative.pulsar.handlers.mqtt.utils.MqttMessageUtils;
import io.streamnative.pulsar.handlers.mqtt.utils.NettyUtils;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.broker.service.BrokerServiceException;

/**
 * Value object to maintain the information of single connection, like ClientID, Channel, and clean
 * session flag.
 */
@Slf4j
public class Connection {

    @Getter
    private final String clientId;
    @Getter
    private final Channel channel;
    @Getter
    private final boolean cleanSession;

    volatile ConnectionState connectionState = DISCONNECTED;

    private final AtomicReferenceFieldUpdater<Connection, ConnectionState> channelState =
            newUpdater(Connection.class, ConnectionState.class, "connectionState");

    public Connection(String clientId, Channel channel, boolean cleanSession) {
        this.clientId = clientId;
        this.channel = channel;
        this.cleanSession = cleanSession;
    }

    public void sendConnAck() {
        boolean ret = assignState(DISCONNECTED, CONNECT_ACK);
        if (ret) {
            MqttConnAckMessage ackMessage = MqttMessageUtils.connAck(MqttConnectReturnCode.CONNECTION_ACCEPTED);
            channel.writeAndFlush(ackMessage).addListener(future -> {
                if (future.isSuccess()) {
                    if (log.isDebugEnabled()) {
                        log.debug("The CONNECT message has been processed. CId={}", clientId);
                    }
                    assignState(CONNECT_ACK, ESTABLISHED);
                    log.info("current connection state : {}", channelState.get(this));
                }
            });
        } else {
            log.warn("Unable to assign the state from : {} to : {} for CId={}, close channel",
                    DISCONNECTED, CONNECT_ACK, clientId);
            channel.close();
        }
    }

    public void removeConsumers() {
        Map<String, Pair<MQTTCommonConsumer, MQTTVirtualConsumer>> topicSubscriptions = NettyUtils
                .getTopicSubscriptions(channel);
        // For producer doesn't bind subscriptions
        if (topicSubscriptions != null) {
            topicSubscriptions.forEach((k, v) -> {
                try {
                    v.getKey().remove(k, v.getValue());
                } catch (Exception ex) {
                    log.warn("Topic [{}] remove consumer {} error", k, v.getRight(), ex);
                }
            });
        }
    }

    public void removeSubscriptions() {
        removeConsumers();
        if (cleanSession) {
            /*Map<String, Pair<MQTTCommonConsumer, MQTTVirtualConsumer>> topicSubscriptions = NettyUtils
                    .getTopicSubscriptions(channel);
            // For producer doesn't bind subscriptions
            if (topicSubscriptions != null) {
                topicSubscriptions.forEach((k, v) -> {
                    k.unsubscribe(NettyUtils.getClientId(channel));
                    v.getLeft().delete();
                });
            }*/
        }
    }

    public void close() {
        close(false);
    }

    public void close(boolean force) {
        if (log.isInfoEnabled()) {
            log.info("Closing connection. clientId = {}.", clientId);
        }
        if (!force) {
            assignState(ESTABLISHED, DISCONNECTED);
        }
        this.channel.close();
    }

    private boolean assignState(ConnectionState expected, ConnectionState newState) {
        if (log.isDebugEnabled()) {
            log.debug(
                    "Updating state of connection. CId = {}, currentState = {}, "
                            + "expectedState = {}, newState = {}.",
                    clientId,
                    channelState.get(this),
                    expected,
                    newState);
        }
        boolean ret = channelState.compareAndSet(this, expected, newState);
        if (!ret) {
            log.error(
                    "Unable to update state of connection."
                            + " CId = {}, currentState = {}, expectedState = {}, newState = {}.",
                    clientId,
                    channelState.get(this),
                    expected,
                    newState);
        }
        return ret;
    }

    @Override
    public String toString() {
        return "Connection{" + "clientId=" + clientId + ", channel=" + channel
                + ", cleanSession=" + cleanSession + ", state="
                + channelState.get(this) + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Connection that = (Connection) o;
        return Objects.equals(clientId, that.clientId) && Objects.equals(channel, that.channel);
    }

    @Override
    public int hashCode() {
        return Objects.hash(clientId, channel);
    }

    /**
     * Connection state.
     */
    public enum ConnectionState {
        DISCONNECTED,
        CONNECT_ACK,
        ESTABLISHED,
    }
}
