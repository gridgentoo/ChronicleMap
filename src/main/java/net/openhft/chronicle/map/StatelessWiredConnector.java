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
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.wire.TextWire;
import net.openhft.chronicle.wire.Wire;
import net.openhft.lang.io.DirectStore;
import net.openhft.lang.io.NativeBytes;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.List;

import static java.nio.channels.SelectionKey.OP_READ;
import static java.nio.channels.SelectionKey.OP_WRITE;

/**
 * @author Rob Austin.
 */
class StatelessWiredConnector<K extends BytesMarshallable, V extends BytesMarshallable> {

    private static final Logger LOG = LoggerFactory.getLogger(StatelessWiredConnector.class);


    private static final Field BB_ADDRESS;
    private static final Field BB_CAPACITY;
    private static final Method NATIVE_ADDRESS;
    private static final Method NATIVE_CAPACITY;


    static {
        Field bbAdress = null, bbCapacity = null;
        Method nAddress = null, nCapacity = null;
        try {

            bbAdress = Buffer.class.getDeclaredField("address");
            bbAdress.setAccessible(true);
            bbCapacity = Buffer.class.getDeclaredField("capacity");
            bbCapacity.setAccessible(true);

            nAddress = NativeBytes.class.getDeclaredMethod("address", long.class);
            nAddress.setAccessible(true);
            nCapacity = NativeBytes.class.getDeclaredMethod("capacity", long.class);
            nCapacity.setAccessible(true);

        } catch (Exception e) {
            LOG.error("", e);
        }

        BB_ADDRESS = bbAdress;
        BB_CAPACITY = bbCapacity;
        NATIVE_ADDRESS = nAddress;
        NATIVE_CAPACITY = nCapacity;
    }

    private final NativeBytes inLangBytes = new NativeBytes(0, 0);
    private final TextWire inWire = new TextWire(Bytes.elasticByteBuffer());

    private boolean handshingComplete;
    private final TextWire outWire = new TextWire(Bytes.elasticByteBuffer());
    // private final byte identifier;
    private ArrayList<BytesChronicleMap> bytesChronicleMaps = new ArrayList<>();
    private final byte localIdentifier;
    private final StringBuilder methodName = new StringBuilder();
    private final ByteBuffer resultBuffer = ByteBuffer.allocateDirect(64);
    private ByteBuffer inLanByteBuffer = ByteBuffer.allocateDirect(64);
    private final MapIOBuffer mapIOBuffer = new MapIOBuffer() {

        @Override
        public void ensureBufferSize(long size) {

            if (inLanByteBuffer.capacity() < size)
                // reallocate the buffer in chunks of OS.pageSize() bytes
                inLanByteBuffer = ByteBuffer.allocateDirect(OS.pageSize() * (1 + (int) (size / OS.pageSize())));


            outWire.bytes().ensureCapacity(size);
            try {

                // move the lang byte buffer  so that its pointing to the same memory as the wireBuffer
                long address = BB_ADDRESS.getLong(inLanByteBuffer);
                NATIVE_ADDRESS.invoke(inLangBytes, address);
                NATIVE_CAPACITY.invoke(inLangBytes, size + address);
                inLangBytes.limit(size);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }

        }

        /**
         * maps the outWire.bytes() to lang bytes
         *
         * @return lang bytes that represent the {@code outWire.bytes()}
         */
        @Override
        public net.openhft.lang.io.Bytes in() {
            return inLangBytes;
        }
    };
    private long timestamp;
    private short channelId;
    @Nullable
    // maybe null if the server has not been set up to use channel
    private List<Replica> channelList;
    private byte remoteIdentifier;
    private Bytes resultBytes = Bytes.wrap(resultBuffer);
    private SelectionKey key;

    public StatelessWiredConnector(@Nullable List<Replica> channelList, byte localIdentifier) {
        this.channelList = channelList;

        // final TextWire inWire = new TextWire(toChronicleBytes(inBytes));
        //  this.identifier = inWire.read(() -> "IDENTIFIER").int8();
        this.localIdentifier = localIdentifier;
    }

    public void onRead(SocketChannel socketChannel, SelectionKey key) throws IOException {
        this.key = key;

        if (!handshingComplete)
            outWire.bytes().writeByte(localIdentifier);

        final int len = readSocket(socketChannel);
        assert inWire.bytes().remaining() < 999;
        if (len == -1) {
            socketChannel.close();
            return;
        }

        if (len == 0) {
            System.out.println("nothing read");
            return;
        }

        assert inWire.bytes().remaining() < 999;

        System.out.println("remaining=>" + inWire.bytes().remaining());

        while (inWire.bytes().remaining() > 4) {

            final long limit = inWire.bytes().limit();

            if (nextWireMessage() == null) {
                if (shouldCompactInBufffer())
                    compactInBuffer();
                else if (shouldClearInBuffer())
                    clearInBuffer();
                return;
            }

            if (!handshingComplete) {
                System.out.println("doHandshaking");

                onHandShaking();
                System.out.println("end-doHandshaking");
            } else {
                System.out.println("onEvent");
                onEvent();
                System.out.println("end-onEvent");
            }

            System.out.println("limit=>" + outWire.bytes().limit());
            assert inWire.bytes().remaining() < 999;
            inWire.bytes().limit(limit);
            assert inWire.bytes().remaining() < 999;
        }
    }

    private void clearInBuffer() {
        System.out.println("clear");
        inWire.bytes().clear();
        inWireBuffer().clear();
    }

    private boolean shouldClearInBuffer() {
        return inWire.bytes().position() == inWireBuffer().position();
    }

    /**
     * @return true if remaining space is less than 50%
     */
    private boolean shouldCompactInBufffer() {
        return inWire.bytes().position() > 0 && inWire.bytes().remaining() < (inWireBuffer().capacity() / 2);
    }

    public void onWrite(SocketChannel socketChannel, SelectionKey key) throws IOException {

        ((ByteBuffer) outWire.bytes().underlyingObject()).limit((int) outWire.bytes().position());
        socketChannel.write((ByteBuffer) outWire.bytes().underlyingObject());

        if (((ByteBuffer) outWire.bytes().underlyingObject()).remaining() == 0) {
            ((ByteBuffer) outWire.bytes().underlyingObject()).clear();
            outWire.bytes().clear();
            key.interestOps(OP_READ);
        }

    }

    private void compactInBuffer() {
        System.out.println("compacting");
        inWireBuffer().position((int) inWire.bytes().position());
        inWireBuffer().limit(inWireBuffer().position());
        inWireBuffer().compact();

        inWire.bytes().position(0);
        inWire.bytes().limit(0);
    }

    private ByteBuffer inWireBuffer() {
        return (ByteBuffer) inWire.bytes().underlyingObject();
    }

    private int readSocket(SocketChannel socketChannel) throws IOException {
        ByteBuffer dst = inWireBuffer();
        int len = socketChannel.read(dst);
        System.out.println("len=" + len);
        int readUpTo = dst.position();
        inWire.bytes().limit(readUpTo);
        return len;
    }

    private Wire nextWireMessage() {
        if (inWire.bytes().remaining() < 4)
            return null;

        final Bytes<?> bytes = inWire.bytes();
        int size = bytes.readInt(bytes.position());

        inWire.bytes().ensureCapacity(bytes.position() + size);

        if (bytes.remaining() < size) {

            assert size < 100000;
            return null;
        }

        inWire.bytes().limit(bytes.position() + size);

        // skip the size
        inWire.bytes().skip(4);

        return inWire;
    }

    void onHandShaking() {
        remoteIdentifier = inWire.read(() -> "IDENTIFIER").int8();
        handshingComplete = true;
    }

    @Nullable
    Work onEvent() {

        // it is assumed by this point that the buffer has all the bytes in it for this message
        long transactionId = inWire.read(() -> "TRANSACTION_ID").int64();
        timestamp = inWire.read(() -> "TIME_STAMP").int64();
        channelId = inWire.read(() -> "CHANNEL_ID").int16();
        inWire.read(() -> "METHOD_NAME").text(methodName);

        // for the length
        outWire.bytes().skip(4);

        // write the transaction id
        outWire.write(() -> "TRANSACTION_ID").int64(transactionId);

        if ("PUT".contentEquals(methodName))
                return put();
        else
            throw new IllegalStateException("unsupported event=" + methodName);

    }


    private void writeLength(@NotNull final Wire textWire) {

        if (textWire.bytes().position() > Integer.MAX_VALUE)
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

    /**
     * gets the map for this channel id
     *
     * @param channelId the ID of the map
     * @return the chronicle map with this {@code channelId}
     */
    private ReplicatedChronicleMap map(short channelId) {

        // todo this cast is a bit of a hack, improve later
        final ReplicatedChronicleMap replicas =
                (ReplicatedChronicleMap) channelList.get(channelId);

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

        final BytesChronicleMap bytesChronicleMap = (channelId < bytesChronicleMaps.size())
                ? bytesChronicleMaps.get(channelId - 1)
                : null;

        if (bytesChronicleMap != null)
            return bytesChronicleMap;

        // grow the array
        for (int i = bytesChronicleMaps.size(); i <= channelId; i++) {
            bytesChronicleMaps.add(null);
        }

        final ReplicatedChronicleMap delegate = map(channelId);
        final BytesChronicleMap element = new BytesChronicleMap(delegate);
        bytesChronicleMaps.set(channelId, element);
        return element;

    }

    @Nullable
    private Work put() {

        final net.openhft.lang.io.Bytes reader = toReader(inWire, "ARG_1", "ARG_2");


        // skip 4 bytes for the size
        outWire.bytes().skip(4);

        final BytesChronicleMap bytesMap = bytesMap(channelId);

        try {

            inLanByteBuffer.clear();
            inLangBytes.clear();

            // todo
            net.openhft.lang.io.Bytes result = bytesMap.put(reader, reader, timestamp, remoteIdentifier);


            // clear the buffer we just used to write the lang bytes
            inLanByteBuffer.clear();
            inLangBytes.clear();


            if (result != null && result.remaining() > 0) {

                BB_ADDRESS.set(resultBuffer, result.address());
                BB_CAPACITY.set(resultBuffer, result.capacity());
                resultBytes.limit(result.capacity());

                outWire.write(() -> "RESULT_IS_NULL").bool(false);
                outWire.write(() -> "RESULT").bytes(resultBytes);
            } else {
                outWire.write(() -> "RESULT_IS_NULL").bool(true);
            }

            key.interestOps(OP_WRITE | OP_READ);

        } catch (Throwable e) {

            // todo remove
            e.printStackTrace();

            // move back to the start
            outWire.bytes().position(2);

            return sendException(outWire, e);
        } finally {
            bytesMap.output = null;
            writeLength(outWire);
        }

        return null;
    }


}
