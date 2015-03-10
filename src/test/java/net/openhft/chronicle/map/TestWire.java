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

package net.openhft.chronicle.map;


import net.openhft.chronicle.hash.replication.ReplicationHub;
import net.openhft.chronicle.hash.replication.TcpTransportAndNetworkConfig;
import net.openhft.chronicle.wire.Marshallable;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireOut;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.*;

import static java.util.concurrent.TimeUnit.SECONDS;
import static net.openhft.chronicle.map.ChronicleMapBuilder.of;
import static org.junit.Assert.*;

/**
 * Created by Rob Austin
 */
public class TestWire {

    public static int SERVER_PORT = 8086;
    private ChronicleMap<byte[], byte[]> map1a;
    private ChronicleMap<byte[], byte[]> map2a;
    private ChronicleMap<byte[], byte[]> map3a;
    private ReplicationHub hubA;


    @Test
    public void testUsingDetails() throws IOException, InterruptedException {

        {
            final TcpTransportAndNetworkConfig tcpConfig = TcpTransportAndNetworkConfig
                    .of(++SERVER_PORT)
                    .heartBeatInterval(1, SECONDS);

            hubA = ReplicationHub.builder().tcpTransportAndNetwork(tcpConfig)
                    .createWithId((byte) 1);

            // this is how you add maps after the custer is created
            map1a = of(byte[].class, byte[].class)
                    .instance().replicatedViaChannel(hubA.createChannel((short) 1)).create();


            byte identifier = (byte) 2;


            final WiredChronicleMapStatelessClientBuilder<String, Details> builder =
                    new WiredChronicleMapStatelessClientBuilder<>(
                            new InetSocketAddress("localhost", SERVER_PORT),
                            String.class,
                            Details.class,
                            (short) 1);

            builder.identifier(identifier);
            final ChronicleMap<String, Details> detailsMap = builder.create();


            Details details = detailsMap.get("Colours");

            // todo add some sort of cross node locking
            if (details == null) {
                short channelID = (short) 2;
                ((WiredStatelessChronicleMap) detailsMap).createChannel(channelID);
                details = new Details(String.class, String.class, channelID);
                detailsMap.put("Colours", details);
            }


            ChronicleMap<String, String> colours = new WiredChronicleMapStatelessClientBuilder<String, String>(
                    builder.hub(),
                    details.keyClass,
                    details.valueClass,
                    details.channelID).hub(builder.hub()).create();

            colours.put("Rob", "Blue");

            assertEquals(1, colours.size());


            Details result = detailsMap.get("Colours");
            Assert.assertEquals(2, result.channelID);
            map1a.close();
        }

    }


    @Test
    public void testCreateChannel() throws IOException, InterruptedException {

        {
            final TcpTransportAndNetworkConfig tcpConfig = TcpTransportAndNetworkConfig
                    .of(++SERVER_PORT)
                    .heartBeatInterval(1, SECONDS);

            hubA = ReplicationHub.builder().tcpTransportAndNetwork(tcpConfig)
                    .createWithId((byte) 1);

            // this is how you add maps after the custer is created
            map1a = of(byte[].class, byte[].class)
                    .instance().replicatedViaChannel(hubA.createChannel((short) 1)).create();


            byte identifier = (byte) 2;

            final WiredChronicleMapStatelessClientBuilder<String, String> builder =
                    new WiredChronicleMapStatelessClientBuilder<>(
                            new InetSocketAddress("localhost", SERVER_PORT),
                            String.class,
                            String.class,
                            (short) 1);

            builder.identifier(identifier);
            final ChronicleMap<String, String> statelessMap = builder.create();
            ((WiredStatelessChronicleMap) statelessMap).createChannel((short) 2);

            WiredChronicleMapStatelessClientBuilder<String, String> localhost = new WiredChronicleMapStatelessClientBuilder<String, String>(
                    new InetSocketAddress("localhost", SERVER_PORT),
                    String.class,
                    String.class,
                    (short) 2).hub(builder.hub());

            ChronicleMap<String, String> map2 = localhost.create();

            map2.put("hello", "world");

            assertEquals(1, map2.size());

            map1a.close();
        }

    }

    @Test
    public void testSimplePutStringWithChannels() throws IOException, InterruptedException {

        {
            final TcpTransportAndNetworkConfig tcpConfig = TcpTransportAndNetworkConfig
                    .of(++SERVER_PORT)
                    .heartBeatInterval(1, SECONDS);

            hubA = ReplicationHub.builder().tcpTransportAndNetwork(tcpConfig)
                    .createWithId((byte) 1);

            // this is how you add maps after the custer is created
            map1a = of(byte[].class, byte[].class)
                    .instance().replicatedViaChannel(hubA.createChannel((short) 1)).create();

            short channelID = (short) 1;
            byte identifier = (byte) 2;

            final WiredChronicleMapStatelessClientBuilder<String, String> builder =
                    new WiredChronicleMapStatelessClientBuilder<>(
                            new InetSocketAddress("localhost", SERVER_PORT),
                            String.class,
                            String.class,
                            channelID);

            builder.identifier(identifier);
            builder.putReturnsNull(true);

            try (ChronicleMap<String, String> statelessMap = builder.create()) {
                statelessMap.put("Hello", "World");
                assertEquals("World", statelessMap.get("Hello"));
                assertTrue("World", statelessMap.containsKey("Hello"));
                assertFalse(statelessMap.isEmpty());
            }

            map1a.close();

        }


    }


    @Test
    public void testVersion() throws IOException, InterruptedException {

        final TcpTransportAndNetworkConfig tcpConfig = TcpTransportAndNetworkConfig
                .of(++SERVER_PORT)
                .heartBeatInterval(1, SECONDS);

        hubA = ReplicationHub.builder().tcpTransportAndNetwork(tcpConfig)
                .createWithId((byte) 1);

        // this is how you add maps after the custer is created
        map1a = of(byte[].class, byte[].class)
                .instance().replicatedViaChannel(hubA.createChannel((short) 1)).create();

        short channelID = (short) 1;
        byte identifier = (byte) 2;

        final WiredChronicleMapStatelessClientBuilder<String, String> builder =
                new WiredChronicleMapStatelessClientBuilder<>(
                        new InetSocketAddress("localhost", SERVER_PORT),
                        String.class,
                        String.class,
                        channelID);

        builder.identifier(identifier);
        builder.putReturnsNull(true);

        try (ChronicleMap<String, String> statelessMap = builder.create()) {
            String s = ((WiredStatelessChronicleMap) statelessMap).serverApplicationVersion();
            assertNotNull(s);

        }
        map1a.close();

    }


    @Test
    public void testLongSize() throws IOException, InterruptedException {

        {
            final TcpTransportAndNetworkConfig tcpConfig = TcpTransportAndNetworkConfig
                    .of(++SERVER_PORT)
                    .heartBeatInterval(1, SECONDS);

            hubA = ReplicationHub.builder().tcpTransportAndNetwork(tcpConfig)
                    .createWithId((byte) 1);

            // this is how you add maps after the custer is created
            map1a = of(byte[].class, byte[].class)
                    .instance().replicatedViaChannel(hubA.createChannel((short) 1)).create();

            short channelID = (short) 1;
            byte identifier = (byte) 2;

            final WiredChronicleMapStatelessClientBuilder<String, String> builder =
                    new WiredChronicleMapStatelessClientBuilder<>(
                            new InetSocketAddress("localhost", SERVER_PORT),
                            String.class,
                            String.class,
                            channelID);

            builder.identifier(identifier);
            builder.putReturnsNull(true);

            try (ChronicleMap<String, String> statelessMap = builder.create()) {
                assertTrue(statelessMap.isEmpty());
                statelessMap.put("Hello", "World");
                assertEquals(1, statelessMap.longSize());
                assertFalse(statelessMap.isEmpty());
            }
            map1a.close();

        }

    }


    @Test
    public void testPutMarshallableWithChannels() throws IOException, InterruptedException {

        {
            final TcpTransportAndNetworkConfig tcpConfig = TcpTransportAndNetworkConfig
                    .of(++SERVER_PORT)
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

            final WiredChronicleMapStatelessClientBuilder<String, Details> builder =
                    new WiredChronicleMapStatelessClientBuilder<>(
                            new InetSocketAddress("localhost", SERVER_PORT),
                            String.class,
                            Details.class,
                            (short) 1);

            builder.identifier(identifier);
            builder.putReturnsNull(true);
            final ChronicleMap<String, Details> statelessMap = builder.create();

            final Details expected = new Details();
            expected.keyClass = String.class;
            expected.valueClass = String.class;

            statelessMap.put("FirstMap", expected);

            final Details actual = statelessMap.get("FirstMap");

            assertEquals(expected, actual);

            map1a.close();
            map2a.close();
            map3a.close();
        }

    }


    @Test
    public void testGetNullValue() throws IOException, InterruptedException {

        {
            final TcpTransportAndNetworkConfig tcpConfig = TcpTransportAndNetworkConfig
                    .of(++SERVER_PORT)
                    .heartBeatInterval(1, SECONDS);

            hubA = ReplicationHub.builder().tcpTransportAndNetwork(tcpConfig)
                    .createWithId((byte) 1);

            // this is how you add maps after the custer is created
            map1a = of(byte[].class, byte[].class)
                    .instance().replicatedViaChannel(hubA.createChannel((short) 1)).create();


            byte identifier = (byte) 2;

            final WiredChronicleMapStatelessClientBuilder<String, String> builder =
                    new WiredChronicleMapStatelessClientBuilder<>(
                            new InetSocketAddress("localhost", SERVER_PORT),
                            String.class,
                            String.class,
                            (short) 1);

            builder.identifier(identifier);
            final ChronicleMap<String, String> statelessMap = builder.create();

            final String actual = statelessMap.get("FirstMap");
            assertEquals(null, actual);
            map1a.close();
        }

    }

    @Test
    public void testKeySet_Values_EntrySet() throws IOException, InterruptedException {

        {
            final TcpTransportAndNetworkConfig tcpConfig = TcpTransportAndNetworkConfig
                    .of(++SERVER_PORT)
                    .heartBeatInterval(1, SECONDS);

            hubA = ReplicationHub.builder().tcpTransportAndNetwork(tcpConfig)
                    .createWithId((byte) 1);

            // this is how you add maps after the custer is created
            map1a = of(byte[].class, byte[].class)
                    .instance().replicatedViaChannel(hubA.createChannel((short) 1)).create();


            byte identifier = (byte) 2;

            final WiredChronicleMapStatelessClientBuilder<String, String> builder =
                    new WiredChronicleMapStatelessClientBuilder<>(
                            new InetSocketAddress("localhost", SERVER_PORT),
                            String.class,
                            String.class,
                            (short) 1);

            builder.identifier(identifier);
            final ChronicleMap<String, String> statelessMap = builder.create();

            statelessMap.put("Key1", "Value1");
            statelessMap.put("Key2", "Value2");


            Set<String> keys = statelessMap.keySet();

            assertEquals(2, keys.size());
            assertTrue(keys.contains("Key2"));
            assertTrue(keys.contains("Key1"));

            Collection<String> values = statelessMap.values();

            assertEquals(2, values.size());
            assertTrue(values.contains("Value1"));
            assertTrue(values.contains("Value2"));


            Set<Map.Entry<String, String>> entries = statelessMap.entrySet();

            assertEquals(2, values.size());
            assertTrue(entries.contains(Collections.singletonMap("Key1", "Value1").entrySet().iterator().next()));
            assertTrue(entries.contains(Collections.singletonMap("Key2", "Value2").entrySet().iterator().next()));


            map1a.close();
        }

    }


    @Test
    public void testPutAll() throws IOException, InterruptedException {

        {
            final TcpTransportAndNetworkConfig tcpConfig = TcpTransportAndNetworkConfig
                    .of(++SERVER_PORT)
                    .heartBeatInterval(1, SECONDS);

            hubA = ReplicationHub.builder().tcpTransportAndNetwork(tcpConfig)
                    .createWithId((byte) 1);

            // this is how you add maps after the custer is created
            map1a = of(byte[].class, byte[].class)
                    .instance().replicatedViaChannel(hubA.createChannel((short) 1)).create();


            byte identifier = (byte) 2;

            final WiredChronicleMapStatelessClientBuilder<String, String> builder =
                    new WiredChronicleMapStatelessClientBuilder<>(
                            new InetSocketAddress("localhost", SERVER_PORT),
                            String.class,
                            String.class,
                            (short) 1);

            builder.identifier(identifier);
            final ChronicleMap<String, String> statelessMap = builder.create();


            Map<String, String> map = new HashMap<>();
            map.put("Key1", "Value1");
            map.put("Key2", "Value2");

            statelessMap.putAll(map);

            Set<String> keys = statelessMap.keySet();

            assertEquals(2, keys.size());
            assertTrue(keys.contains("Key2"));
            assertTrue(keys.contains("Key1"));

            Collection<String> values = statelessMap.values();

            assertEquals(2, values.size());
            assertTrue(values.contains("Value1"));
            assertTrue(values.contains("Value2"));

            map1a.close();
        }

    }

    public static class Details implements Marshallable {

        Class keyClass;
        Class valueClass;
        short channelID;


        public Details() {
        }


        public Details(Class<String> keyClass, Class<String> valueClass, short channelID) {
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
