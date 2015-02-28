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

/**
 * Created by Rob Austin
 */

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.BytesMarshallable;
import net.openhft.chronicle.wire.TextWire;
import net.openhft.chronicle.wire.Wire;
import net.openhft.lang.io.DirectStore;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Rob Austin.
 */
class StatelessWiredConnector<K extends BytesMarshallable, V extends BytesMarshallable> {


    private boolean handshingComplete;
    // private final byte identifier;

    @Nullable
    // maybe null if the server has not been set up to use channel
    private List<Replica> channelList;

    private ArrayList<BytesChronicleMap> bytesChronicleMaps = new ArrayList<>();
    private TextWire inWire = new TextWire(Bytes.elasticByteBuffer());
    private TextWire outWire = new TextWire(Bytes.elasticByteBuffer());

    private byte identifier;
    private long transactionId;
    private long timestamp;
    private short channelId;
    private String methodName;

    public StatelessWiredConnector(@Nullable List<Replica> channelList) {
        this.channelList = channelList;
        // final TextWire inWire = new TextWire(toChronicleBytes(inBytes));
        //  this.identifier = inWire.read(() -> "IDENTIFIER").int8();
    }

    public void onRead(SocketChannel socketChannel, SelectionKey key) throws IOException {
        socketChannel.read(inWireBuffer());

        if (nextWireMessage() == null)
            return;

        if (handshingComplete) {
            onEvent();
        } else
            onHandShaking();

    }

    public void onWrite(SocketChannel socketChannel, SelectionKey key) throws IOException {
        socketChannel.write((ByteBuffer) outWire.bytes());
        if (outWire.bytes().remaining() == 0) {
            ((ByteBuffer) outWire.bytes().underlyingObject()).clear();
            outWire.bytes().clear();
        }
    }


    private Wire nextWireMessage() {
        if (inWireBuffer().remaining() < 4)
            return null;

        final Bytes<?> bytes = inWire.bytes();
        int size = bytes.readInt(bytes.position());

        inWire.bytes().ensureCapacity(bytes.position() + size);

        if (bytes.remaining() < size) {
            return null;
        }

        inWire.bytes().limit(bytes.position() + size);
        inWire.bytes().position(inWire.bytes().position() + 2);
        return inWire;
    }

    private ByteBuffer inWireBuffer() {
        return (ByteBuffer) inWire.bytes().underlyingObject();
    }


    void onHandShaking() {
        identifier = inWire.read(() -> "IDENTIFIER").int8();
        handshingComplete = true;
    }

    @Nullable
    Work onEvent() {

        // it is assumed by this point that the buffer has all the bytes in it for this message
        transactionId = inWire.read(() -> "TRANSACTION_ID").int64();
        timestamp = inWire.read(() -> "TIME_STAMP").int64();
        channelId = inWire.read(() -> "CHANNEL_ID").int16();
        methodName = inWire.read(() -> "METHOD_NAME").text();

        // for the length
        outWire.bytes().skip(4);

        // write the transaction id
        outWire.write(() -> "TRANSACTION_ID").int64(transactionId);

        switch (methodName) {

            case "PUT":
                return put();

            default:
                throw new IllegalStateException("unsupported event=" + methodName);
        }
    }

    /**
     * gets the map for this channel id
     *
     * @param channelId the ID of the map
     * @return the chronicle map with this {@code channelId}
     */
    private ReplicatedChronicleMap map(short channelId) {

        // todo this cast is a bit of a hack, improve later
        final ReplicatedChronicleMap replicas =
                (ReplicatedChronicleMap) channelList.get(channelId - 1);

        if (replicas != null)
            return replicas;

        throw new IllegalStateException();
    }

    /**
     * this is used to push the data straight into the entry in memory
     *
     * @param channelId the ID of the map
     * @return a BytesChronicleMap used to update the memory which holds the chronicle map
     */
    private BytesChronicleMap bytesMap(short channelId) {

        final BytesChronicleMap bytesChronicleMap = bytesChronicleMaps.get(channelId - 1);

        if (bytesChronicleMap != null)
            return bytesChronicleMap;

        // grow the array
        for (int i = bytesChronicleMaps.size(); i < channelId; i++) {
            bytesChronicleMaps.add(null);
        }

        final ReplicatedChronicleMap delegate = map(channelId);
        final BytesChronicleMap element = new BytesChronicleMap(delegate);
        bytesChronicleMaps.set(channelId - 1, element);
        return element;

    }


    private void writeLength(@NotNull final Wire textWire,
                             @NotNull final TcpReplicator.TcpSocketChannelEntryWriter writer) {

        if (writer.in().position() > Integer.MAX_VALUE)
            throw new IllegalStateException("position too large");

        // write the size
        textWire.bytes().writeInt(0, (int) textWire.bytes().position());
    }

    /**
     * creates a lang buffer that holds just the payload of the args
     *
     * @param wire the inbound wire
     * @param args the key names of the {@code wire} args
     * @return a new lang buffer containing the bytes of the args
     */
    private net.openhft.lang.io.Bytes toReader(@NotNull Wire wire, @NotNull String... args) {

        long inSize = wire.bytes().limit();
        final net.openhft.lang.io.Bytes bytes = DirectStore.allocate(inSize).bytes();

        // copy the bytes to the reader
        for (final String field : args) {
            wire.read(() -> field).bytes(source -> {
                bytes.writeStopBit(source.length);
                bytes.write(source);
            });
        }

        return bytes.flip();
    }

    @Nullable
    private Work sendException(@NotNull Wire wire, @NotNull Throwable e) {
        final StringWriter sw = new StringWriter();
        final PrintWriter pw = new PrintWriter(sw);
        e.printStackTrace(pw);
        wire.write(() -> "EXCEPTION").text(sw.toString());
        return null;
    }

    @Nullable
    private Work put() {

        final net.openhft.lang.io.Bytes reader = toReader(inWire, "ARG_1", "ARG_2");


        // move the lang bytes to the same location as the chronicle bytes


        // todo  - in comment code below
     /*   writer.in().position(out.bytes().position());

        final BytesChronicleMap bytesMap = bytesMap(channelId);
        bytesMap.output = writer;

        try {
            bytesMap.put(reader, reader, timestamp, identifier);
        } catch (Throwable e) {
            // move back to the start
            out.bytes().position(2);
            return sendException(out, e);
        } finally {
            bytesMap.output = null;
            writeLength(out, writer);
        }*/

        return null;
    }


}
