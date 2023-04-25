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

import io.streamnative.pulsar.handlers.mqtt.support.MQTTVirtualConsumer;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.MessageId;

/**
 * Outstanding packet that the broker sent to clients.
 */
public class OutstandingVirtualPacket {

    private final Consumer<byte[]> consumer;
    private final int packetId;
    private final MessageId messageId;

    public OutstandingVirtualPacket(Consumer<byte[]> consumer, int packetId,MessageId messageId) {
        this.consumer = consumer;
        this.packetId = packetId;
        this.messageId = messageId;
    }

    public Consumer<byte[]> getConsumer() {
        return consumer;
    }

    public int getPacketId() {
        return packetId;
    }

    public MessageId getMessageId() {
        return messageId;
    }
}
