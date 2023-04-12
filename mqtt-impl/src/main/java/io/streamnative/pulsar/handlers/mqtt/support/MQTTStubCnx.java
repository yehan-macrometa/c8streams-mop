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
package io.streamnative.pulsar.handlers.mqtt.support;

import io.netty.handler.codec.haproxy.HAProxyMessage;
import io.netty.util.concurrent.DefaultPromise;
import io.netty.util.concurrent.GlobalEventExecutor;
import io.netty.util.concurrent.Promise;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.authentication.AuthenticationDataSource;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.Consumer;
import org.apache.pulsar.broker.service.Producer;
import org.apache.pulsar.broker.service.PulsarCommandSender;
import org.apache.pulsar.broker.service.TransportCnx;

import java.net.InetSocketAddress;
import java.net.SocketAddress;

/**
 * Server cnx for MQTT server.
 */
public class MQTTStubCnx implements TransportCnx {

    PulsarService pulsar;

    public MQTTStubCnx(PulsarService pulsar) {
        this.pulsar = pulsar;
    }

    @Override
    public String getClientVersion() {
        return null;
    }

    @Override
    public SocketAddress clientAddress() {
        return new InetSocketAddress("127.0.0.1", 99);
    }

    @Override
    public BrokerService getBrokerService() {
        return pulsar.getBrokerService();
    }

    @Override
    public PulsarCommandSender getCommandSender() {
        return null;
    }

    @Override
    public boolean isBatchMessageCompatibleVersion() {
        return false;
    }

    @Override
    public String getAuthRole() {
        return null;
    }

    @Override
    public AuthenticationDataSource getAuthenticationData() {
        return null;
    }

    @Override
    public boolean isActive() {
        return true;
    }

    @Override
    public boolean isWritable() {
        return true;
    }

    @Override
    public void completedSendOperation(boolean isNonPersistentTopic, int msgSize) {

    }

    @Override
    public void removedProducer(Producer producer) {

    }

    @Override
    public void closeProducer(Producer producer) {

    }

    @Override
    public void cancelPublishRateLimiting() {

    }

    @Override
    public void cancelPublishBufferLimiting() {

    }

    @Override
    public void disableCnxAutoRead() {

    }

    @Override
    public void enableCnxAutoRead() {

    }

    @Override
    public void execute(Runnable runnable) {

    }

    @Override
    public void removedConsumer(Consumer consumer) {

    }

    @Override
    public void closeConsumer(Consumer consumer) {

    }

    @Override
    public boolean isPreciseDispatcherFlowControl() {
        return false;
    }

    @Override
    public Promise<Void> newPromise() {
        return new DefaultPromise<>(GlobalEventExecutor.INSTANCE);
    }

    @Override
    public boolean hasHAProxyMessage() {
        return false;
    }

    @Override
    public HAProxyMessage getHAProxyMessage() {
        return null;
    }

    @Override
    public String clientSourceAddress() {
        return "127.0.0.1";
    }
}
