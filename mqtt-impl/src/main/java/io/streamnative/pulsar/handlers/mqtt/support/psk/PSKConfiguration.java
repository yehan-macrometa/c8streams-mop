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

package io.streamnative.pulsar.handlers.mqtt.support.psk;

import io.netty.handler.ssl.ApplicationProtocolConfig;
import io.netty.handler.ssl.ApplicationProtocolNames;
import java.io.File;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

/**
 * Psk configuration.
 */
@Getter
public class PSKConfiguration {

    static Set<String> defaultApplicationProtocols = new HashSet<>();

    static Set<String> defaultCiphers = new HashSet<>();

    static Set<String> defaultProtocols = new HashSet<>();

    static {
        defaultApplicationProtocols.add(ApplicationProtocolNames.HTTP_2);
        defaultApplicationProtocols.add(ApplicationProtocolNames.HTTP_1_1);
        defaultApplicationProtocols.add(ApplicationProtocolNames.SPDY_1);
        defaultApplicationProtocols.add(ApplicationProtocolNames.SPDY_2);
        defaultApplicationProtocols.add(ApplicationProtocolNames.SPDY_3);
        defaultApplicationProtocols.add(ApplicationProtocolNames.SPDY_3_1);

        defaultCiphers.add("TLS_ECDHE_PSK_WITH_CHACHA20_POLY1305_SHA256");
        defaultCiphers.add("TLS_ECDHE_PSK_WITH_AES_128_CBC_SHA");
        defaultCiphers.add("TLS_ECDHE_PSK_WITH_AES_256_CBC_SHA");
        defaultCiphers.add("TLS_PSK_WITH_AES_128_CBC_SHA");
        defaultCiphers.add("TLS_PSK_WITH_AES_256_CBC_SHA");

        defaultProtocols.add("TLSv1");
        defaultProtocols.add("TLSv1.1");
        defaultProtocols.add("TLSv1.2");
    }

    static ApplicationProtocolConfig defaultProtocolConfig = new ApplicationProtocolConfig(
            ApplicationProtocolConfig.Protocol.ALPN,
            ApplicationProtocolConfig.SelectorFailureBehavior.NO_ADVERTISE,
            ApplicationProtocolConfig.SelectedListenerFailureBehavior.ACCEPT,
            defaultApplicationProtocols);

    private PSKSecretKeyStore keyStore = new PSKSecretKeyStore();

    @Setter
    private PSKSecretKey secretKey;

    @Setter
    private String identityHint;

    private File identityFile;

    private String identity;

    private Set<String> applicationProtocols = defaultApplicationProtocols;

    private Set<String> ciphers = defaultCiphers;

    private Set<String> protocols = defaultProtocols;

    private ApplicationProtocolConfig protocolConfig = defaultProtocolConfig;

    public void setIdentityFile(String identityFile) {
        if (StringUtils.isNotEmpty(identityFile)) {
            setIdentityFile(new File(identityFile));
        }
    }

    public void setIdentityFile(File identityFile) {
        this.identityFile = identityFile;
        if (identityFile != null) {
            List<PSKSecretKey> pskKeys = PSKUtils.parse(identityFile);
            pskKeys.forEach(item -> {
                item.setHint(identityHint);
                keyStore.addPSKSecretKey(item);
            });
        }
    }

    public void setIdentity(String identity) {
        this.identity = identity;
        if (StringUtils.isNotEmpty(identity)) {
            List<PSKSecretKey> pskKeys = PSKUtils.parse(identity);
            pskKeys.forEach(item -> {
                item.setHint(identityHint);
                keyStore.addPSKSecretKey(item);
            });
        }
    }

    public void setProtocols(Set<String> protocols) {
        if (CollectionUtils.isNotEmpty(protocols)) {
            this.protocols = protocols;
        }
    }

    public void setCiphers(Set<String> ciphers) {
        if (CollectionUtils.isNotEmpty(ciphers)) {
            this.ciphers = ciphers;
        }
    }
}
