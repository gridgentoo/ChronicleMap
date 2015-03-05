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
import net.openhft.chronicle.hash.replication.ReplicationHub;
import net.openhft.chronicle.hash.replication.TcpTransportAndNetworkConfig;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.WiredChronicleMapStatelessClientBuilder;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;

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
    public void testSimplePutStringWithChannels() throws IOException, InterruptedException {

        {
            final TcpTransportAndNetworkConfig tcpConfig = TcpTransportAndNetworkConfig
                    .of(8086)
                    .heartBeatInterval(1, SECONDS);

            hubA = ReplicationHub.builder().tcpTransportAndNetwork(tcpConfig)
                    .createWithId((byte) 1);

            // this is how you add maps after the custer is created
            map1a = of(byte[].class, byte[].class)
                    .instance().replicatedViaChannel(hubA.createChannel((short) 1)).create();

            short channelID = (short) 1;
            byte identifier = (byte) 2;

            final WiredChronicleMapStatelessClientBuilder<String, String> builder = new WiredChronicleMapStatelessClientBuilder<String, String>(new InetSocketAddress("localhost", 8086), String.class, String.class, channelID);
            builder.identifier(identifier);
            builder.putReturnsNull(true);

            try (ChronicleMap<String, String> statelessMap = builder.create()) {
                statelessMap.put("Hello", "World");
                assertEquals("World", statelessMap.get("Hello"));
            }


        }


    }


    @Test
    public void testPutMarshallableWithChannels() throws IOException, InterruptedException {

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

            byte identifier = (byte) 2;

            final WiredChronicleMapStatelessClientBuilder<String, Details> builder = new WiredChronicleMapStatelessClientBuilder<String, Details>(new InetSocketAddress("localhost", 8086), String.class, Details.class, (short) (byte) 1);
            builder.identifier(identifier);
            builder.putReturnsNull(true);
            final ChronicleMap<String, Details> statelessMap = builder.create();

            final Details expected = new Details();
            expected.keyClass = String.class;
            expected.valueClass = String.class;

            statelessMap.put("FirstMap", expected);

            final Details actual = statelessMap.get("FirstMap");

            Assert.assertEquals(expected, actual);

        }

    }

    @Test
    public void testName() throws Exception {

        TextWire wire = new TextWire(net.openhft.chronicle.bytes.Bytes.elasticByteBuffer());

        wire.write(() -> "hello").text("world");
        System.out.println(wire.bytes());
    }

    public static class Details implements Marshallable {

        Class keyClass;
        Class valueClass;
        short channelID;


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

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Details details = (Details) o;

            if (channelID != details.channelID) return false;
            if (!keyClass.equals(details.keyClass)) return false;
            if (!valueClass.equals(details.valueClass)) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = keyClass.hashCode();
            result = 31 * result + valueClass.hashCode();
            result = 31 * result + (int) channelID;
            return result;
        }
    }
}
