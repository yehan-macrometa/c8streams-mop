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

import lombok.Getter;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.MessageId;

import java.util.Objects;

/**
 * Outstanding packet that the broker sent to clients.
 */
@Getter
public class OutstandingVirtualPacket {

    private final Consumer<byte[]> consumer;
    private final int packetId;
    private final MessageId messageId;
    private final int batchIndex;
    
    private final int batchSize;
    
    public OutstandingVirtualPacket(Consumer<byte[]> consumer, int packetId,MessageId messageId) {
        this.consumer = consumer;
        this.packetId = packetId;
        this.messageId = messageId;
        this.batchIndex = -1;
        this.batchSize = -1;
    }
    
    public OutstandingVirtualPacket(Consumer<byte[]> consumer, int packetId,MessageId messageId, int batchIndex, int batchSize) {
        this.consumer = consumer;
        this.packetId = packetId;
        this.messageId = messageId;
        this.batchIndex = batchIndex;
        this.batchSize = batchSize;
    }
    
    public boolean isBatch() {
        return batchIndex != -1;
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        OutstandingVirtualPacket that = (OutstandingVirtualPacket) o;
        return packetId == that.packetId && batchIndex == that.batchIndex && batchSize == that.batchSize;
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(packetId, batchIndex, batchSize);
    }
}
