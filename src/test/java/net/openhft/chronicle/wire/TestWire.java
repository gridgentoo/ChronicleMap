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
import net.openhft.chronicle.map.WiredChronicleMapStatelessClientBuilder;
import net.openhft.lang.io.ByteBufferBytes;
import net.openhft.lang.io.Bytes;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

import static java.nio.ByteBuffer.allocate;
import static java.util.concurrent.TimeUnit.SECONDS;
import static net.openhft.chronicle.map.ChronicleMapBuilder.of;
import static org.junit.Assert.assertEquals;

/**
 * Created by Rob Austin
 */
public class TestWire {


    private ChronicleMap<byte[], byte[]> map1a;

    private static <K, V> ChronicleMap<K, V> localClient(int port,
                                                         byte identifier,
                                                         @NotNull Class keyClass,
                                                         @NotNull Class valueClass,
                                                         short channelID) throws IOException {

        final WiredChronicleMapStatelessClientBuilder<K, V> builder = new WiredChronicleMapStatelessClientBuilder<K, V>(new InetSocketAddress("localhost", port), keyClass, valueClass, channelID);
        builder.identifier(identifier);
        return builder.create();
    }
    private ChronicleMap<byte[], byte[]> map2a;
    private ChronicleMap<byte[], byte[]> map3a;


    private ReplicationHub hubA;


    @Test
    public void test() throws IOException {

        {
            final TcpTransportAndNetworkConfig tcpConfig = TcpTransportAndNetworkConfig
                    .of(8086)
                    .heartBeatInterval(1, SECONDS);

            hubA = ReplicationHub.builder().tcpTransportAndNetwork(tcpConfig)
                    .createWithId((byte) 1);

            // this is how you add maps after the custer is created
            map1a = of(byte[].class, byte[].class)
                    .instance().replicatedViaChannel(hubA.createChannel((short) 2)).create();

            map2a = of(byte[].class, byte[].class)
                    .entries(1000)
                    .instance().replicatedViaChannel(hubA.createChannel((short) 3)).create();

            map3a = of(byte[].class, byte[].class)
                    .entries(1000)
                    .instance().replicatedViaChannel(hubA.createChannel((short) 4)).create();


            byte[] key = new byte[4];
            Bytes keyBuffer = new ByteBufferBytes(ByteBuffer.wrap(key));


            short channelID = (short) 2;
            byte identifier = (byte) 2;
            try (ChronicleMap<String, String> statelessMap = localClient(8086, identifier, String.class, String.class, channelID)) {

                for (int i = 0; i < 10; i++) {
                    try {
                        statelessMap.put("Hello", "World");
                    } catch (Throwable t) {
                        t.printStackTrace();
                    }
                }

                assertEquals(10, statelessMap.size());
            }


            net.openhft.chronicle.bytes.Bytes bytes = BytesStore.wrap(allocate(128)).bytes();
            TextWire textWire = new TextWire(bytes);
            textWire.write(() -> "TYPE").text("TextWire");

        }


    }

    @Test
    public void testName() throws Exception {

        TextWire wire = new TextWire(net.openhft.chronicle.bytes.Bytes.elasticByteBuffer());

        WireOut world = wire.write(() -> "hello").text("world");
        System.out.println(wire.bytes());


    }

    public static class Details implements Marshallable {

        Class keyClass;
        Class valueClass;
        short channelID;

        public Details(Class keyClass, Class valueClass, short channelID) {
            this.keyClass = keyClass;
            this.valueClass = valueClass;
            this.channelID = channelID;
        }

        @Override
        public void writeMarshallable(WireOut wire) {
            wire.write(() -> "keyClass").text(keyClass.getName());
            wire.write(() -> "valueClass").text(valueClass.getName());
            wire.write(() -> "channelID").int16(channelID);
        }

        @Override
        public void readMarshallable(WireIn wire) throws IllegalStateException {
            try {
                keyClass = Class.forName(wire.read(() -> "keyClass").text());
                valueClass = Class.forName(wire.read(() -> "valueClass").text());
                channelID = wire.read(() -> "channelID").int16();
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
