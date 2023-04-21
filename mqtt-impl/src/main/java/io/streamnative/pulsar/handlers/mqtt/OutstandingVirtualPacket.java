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

    private final MQTTVirtualConsumer consumer;
    private final Consumer<byte[]> pulsarConsumer;
    private final int packetId;
    private final long ledgerId;
    private final long entryId;
    private final MessageId messageId;


    public OutstandingVirtualPacket(MQTTVirtualConsumer consumer, Consumer<byte[]> pulsarConsumer, int packetId, long ledgerId, long entryId) {
        this.consumer = consumer;
        this.pulsarConsumer = pulsarConsumer;
        this.packetId = packetId;
        this.ledgerId = ledgerId;
        this.entryId = entryId;
        this.messageId = null;
    }


    public OutstandingVirtualPacket(MQTTVirtualConsumer consumer, Consumer<byte[]> pulsarConsumer, int packetId,MessageId messageId) {
        this.consumer = consumer;
        this.pulsarConsumer = pulsarConsumer;
        this.packetId = packetId;
        this.ledgerId = 0;
        this.entryId = 0;
        this.messageId = messageId;
    }

    public MQTTVirtualConsumer getConsumer() {
        return consumer;
    }

    public Consumer<byte[]> getPulsarConsumer() {
        return pulsarConsumer;
    }

    public int getPacketId() {
        return packetId;
    }

    public long getLedgerId() {
        return ledgerId;
    }

    public long getEntryId() {
        return entryId;
    }

    public MessageId getMessageId() {
        return messageId;
    }
}
