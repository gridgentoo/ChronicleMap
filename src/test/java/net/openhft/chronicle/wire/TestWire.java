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

import junit.framework.Assert;
import net.openhft.chronicle.bytes.BytesStore;
import net.openhft.chronicle.hash.replication.ReplicationHub;
import net.openhft.chronicle.hash.replication.TcpTransportAndNetworkConfig;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.WiredChronicleMapStatelessClientBuilder;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;

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
    public void testPutStringWithChannels() throws IOException {

        {
            final TcpTransportAndNetworkConfig tcpConfig = TcpTransportAndNetworkConfig
                    .of(8086)
                    .heartBeatInterval(1, SECONDS);

            hubA = ReplicationHub.builder().tcpTransportAndNetwork(tcpConfig)
                    .createWithId((byte) 1);

            // this is how you add maps after the custer is created
            map1a = of(byte[].class, byte[].class)
                    .instance().replicatedViaChannel(hubA.createChannel((short) 1)).create();

            map2a = of(byte[].class, byte[].class)
                    .entries(1000)
                    .instance().replicatedViaChannel(hubA.createChannel((short) 2)).create();

            map3a = of(byte[].class, byte[].class)
                    .entries(1000)
                    .instance().replicatedViaChannel(hubA.createChannel((short) 3)).create();


            byte[] key = new byte[4];

            short channelID = (short) 1;
            byte identifier = (byte) 2;
            try (ChronicleMap<String, String> statelessMap = localClient(8086, identifier, String.class, String.class, channelID)) {

                for (int i = 0; i < 10; i++) {
                    try {
                        statelessMap.put("Hello" + i, "World");
                    } catch (Throwable t) {
                        t.printStackTrace();
                    }
                }

                assertEquals(10, map1a.size());

                // check that the bytes maps is correct
                assertEquals("World", new String(map1a.get("Hello1".getBytes())));
            }


            net.openhft.chronicle.bytes.Bytes bytes = BytesStore.wrap(allocate(128)).bytes();
            TextWire textWire = new TextWire(bytes);
            textWire.write(() -> "TYPE").text("TextWire");

        }


    }


    @Test
    public void testPutMarshanleWithChannels() throws IOException {

        {
            final TcpTransportAndNetworkConfig tcpConfig = TcpTransportAndNetworkConfig
                    .of(8086)
                    .heartBeatInterval(1, SECONDS);

            hubA = ReplicationHub.builder().tcpTransportAndNetwork(tcpConfig)
                    .createWithId((byte) 1);

            // this is how you add maps after the custer is created
            map1a = of(byte[].class, byte[].class)
                    .instance().replicatedViaChannel(hubA.createChannel((short) 1)).create();

            map2a = of(byte[].class, byte[].class)
                    .entries(1000)
                    .instance().replicatedViaChannel(hubA.createChannel((short) 2)).create();

            map3a = of(byte[].class, byte[].class)
                    .entries(1000)
                    .instance().replicatedViaChannel(hubA.createChannel((short) 3)).create();


            byte[] key = new byte[4];

            short channelID = (short) 1;
            byte identifier = (byte) 2;


            ChronicleMap<String, Details> statelessMap = localClient(8086, identifier, String.class, Details.class, (byte) 1);
            statelessMap.put("FirstMap", new Details(String.class, String.class, (byte) 1));

            final Details firstMap = statelessMap.get("FirstMap");
            Assert.assertEquals(1, firstMap.channelID);

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
