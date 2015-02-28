/*
 * Copyright 2014 Higher Frequency Trading
 *
 * http://www.higherfrequencytrading.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.openhft.chronicle.wire;

import net.openhft.chronicle.bytes.BytesStore;
import net.openhft.chronicle.hash.replication.ReplicationHub;
import net.openhft.chronicle.hash.replication.TcpTransportAndNetworkConfig;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.util.ShortConsumer;
import net.openhft.lang.io.ByteBufferBytes;
import net.openhft.lang.io.Bytes;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

import static java.nio.ByteBuffer.allocate;
import static java.util.concurrent.TimeUnit.SECONDS;
import static net.openhft.chronicle.map.ChronicleMapBuilder.of;
import static net.openhft.chronicle.map.ChronicleMapStatelessClientBuilder.createClientOf;
import static org.junit.Assert.assertEquals;

/**
 * Created by Rob Austin
 */
public class TestWire {

    public static class Details {
        Class keyClass;
        Class valueClass;
        short channelID;
    }

    private ChronicleMap<String, Details> map1a;
    private ChronicleMap<byte[], byte[]> map2a;
    private ChronicleMap<byte[], byte[]> map3a;


    private ReplicationHub hubA;

    public static <K, V> ChronicleMap<K, V> localClient(int port) throws IOException {
        return createClientOf(new InetSocketAddress("localhost", port));
    }




    @Before
    public void setup() throws IOException {

        {
            final TcpTransportAndNetworkConfig tcpConfig = TcpTransportAndNetworkConfig
                    .of(8086)
                    .heartBeatInterval(1, SECONDS);

            hubA = ReplicationHub.builder().tcpTransportAndNetwork(tcpConfig)
                    .createWithId((byte) 1);

            // this is how you add maps after the custer is created
            map1a = of(String.class, Details.class)
                    .instance().replicatedViaChannel(hubA.createChannel((short) 1)).create();

            map2a = of(byte[].class, byte[].class)
                    .entries(1000)
                    .instance().replicatedViaChannel(hubA.createChannel((short) 2)).create();

            map3a = of(byte[].class, byte[].class)
                    .entries(1000)
                    .instance().replicatedViaChannel(hubA.createChannel((short) 2)).create();


            byte[] key = new byte[4];
            Bytes keyBuffer = new ByteBufferBytes(ByteBuffer.wrap(key));


            try (ChronicleMap<byte[], byte[]> statelessMap = localClient(8086)) {

                for (int i = 0; i < 10; i++) {
                    keyBuffer.clear();
                    keyBuffer.writeInt(i);
                    statelessMap.put(key, key);
                }

                assertEquals(10, statelessMap.size());
            }




            net.openhft.chronicle.bytes.Bytes bytes = BytesStore.wrap(allocate(128)).bytes();
            TextWire textWire = new TextWire(bytes);
            textWire.write(() -> "TYPE").text("TextWire");

        }


    }




}
