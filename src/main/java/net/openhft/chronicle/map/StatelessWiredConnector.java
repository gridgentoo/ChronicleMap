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
import net.openhft.chronicle.wire.ValueIn;
import net.openhft.chronicle.wire.Wire;
import net.openhft.lang.io.ByteBufferBytes;
import net.openhft.lang.io.DirectStore;
import net.openhft.lang.io.IByteBufferBytes;
import net.openhft.lang.io.NativeBytes;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import static java.nio.channels.SelectionKey.OP_READ;
import static java.nio.channels.SelectionKey.OP_WRITE;

/**
 * @author Rob Austin.
 */
class StatelessWiredConnector<K extends BytesMarshallable, V extends BytesMarshallable> {

    private static final Logger LOG = LoggerFactory.getLogger(StatelessWiredConnector.class);
    private final NativeBytes inLangBytes = new NativeBytes(0, 0);

    private OutMessageAdapter outMessageAdapter = new OutMessageAdapter();

    public void onWrite(SocketChannel socketChannel, SelectionKey key) throws IOException {


        ((ByteBuffer) outWire.bytes().underlyingObject()).limit((int) outWire.bytes().position());
        System.out.println(Bytes.toHex(outWire.bytes().flip()));
        socketChannel.write((ByteBuffer) outWire.bytes().underlyingObject());

        if (((ByteBuffer) outWire.bytes().underlyingObject()).remaining() == 0) {
            ((ByteBuffer) outWire.bytes().underlyingObject()).clear();
            outWire.bytes().clear();
            key.interestOps(OP_READ);
            outMessageAdapter.clear();
        }

    }

    private final TextWire inWire = new TextWire(Bytes.elasticByteBuffer());

    private final TextWire outWire = new TextWire(Bytes.elasticByteBuffer());
    // private final byte identifier;
    private ArrayList<BytesChronicleMap> bytesChronicleMaps = new ArrayList<>();
    private final byte localIdentifier;
    private final StringBuilder methodName = new StringBuilder();
    //private final ByteBuffer resultBuffer = ByteBuffer.allocateDirect(64);
    private ByteBuffer inLanByteBuffer = ByteBuffer.allocateDirect(64);

    private long timestamp;
    private short channelId;
    @Nullable
    // maybe null if the server has not been set up to use channel
    private List<Replica> channelList;
    private byte remoteIdentifier;

    // todo improve later, dont make fixed sie
    private Bytes resultLangBytes = Bytes.wrap(ByteBuffer.allocate(1024));

    private SelectionKey key;
    private long transactionId;

    public StatelessWiredConnector(@Nullable List<Replica> channelList, byte localIdentifier) {
        this.channelList = channelList;
        this.localIdentifier = localIdentifier;
    }


    public void onRead(SocketChannel socketChannel, SelectionKey key) throws IOException {
        this.key = key;

        final int len = readSocket(socketChannel);

        if (len == -1) {
            socketChannel.close();
            return;
        }

        if (len == 0)
            return;

        while (inWire.bytes().remaining() > 4) {

            final long limit = inWire.bytes().limit();

            if (nextWireMessage() == null) {
                if (shouldCompactInBufffer())
                    compactInBuffer();
                else if (shouldClearInBuffer())
                    clearInBuffer();
                return;
            }

            long nextPosition = inWire.bytes().limit();
            try {
                onEvent();
            } finally {
                inWire.bytes().position(nextPosition);
                inWire.bytes().limit(limit);
            }
        }
    }

    private void clearInBuffer() {
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


    private void compactInBuffer() {
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


    @Nullable
    Work onEvent() {

        // it is assumed by this point that the buffer has all the bytes in it for this message
        transactionId = inWire.read(() -> "TRANSACTION_ID").int64();
        timestamp = inWire.read(() -> "TIME_STAMP").int64();
        channelId = inWire.read(() -> "CHANNEL_ID").int16();
        inWire.read(() -> "METHOD_NAME").text(methodName);

        if ("PUT_WITHOUT_ACC".contentEquals(methodName))

            return writeVoid(bytesMap -> {
                final net.openhft.lang.io.Bytes reader = toReader(inWire, "ARG_1", "ARG_2");
                bytesMap.put(reader, reader, timestamp, remoteIdentifier);
            });


        // for the length
        long markStart = outWire.bytes().position();
        outWire.bytes().skip(4);

        try {

            // write the transaction id
            outWire.write(() -> "TRANSACTION_ID").int64(transactionId);

            if ("PUT".contentEquals(methodName))
                return writeValue(bytesMap -> {
                    final net.openhft.lang.io.Bytes reader = toReader(inWire, "ARG_1", "ARG_2");
                    bytesMap.put(reader, reader, timestamp, remoteIdentifier);
                });

            else if ("GET".contentEquals(methodName))
                return writeValue(b -> b.get(toReader(inWire, "ARG_1")));

            else if ("LONG_SIZE".contentEquals(methodName))
                return write(b -> outWire.write(() -> "RESULT").int64(b.longSize()));

            else if ("IS_EMPTY".contentEquals(methodName))
                return write(b -> outWire.write(() -> "RESULT").bool(b.isEmpty()));

            else
                throw new IllegalStateException("unsupported event=" + methodName);
        } finally {
            int len = (int) (outWire.bytes().position() - markStart);
            outWire.bytes().writeInt(markStart, len);
        }

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

            ValueIn read = wire.read(() -> field);
            long fieldLength = read.readLength();

            long endPos = wire.bytes().position() + fieldLength;
            long limit = wire.bytes().limit();

            try {

                final Bytes source = wire.bytes();
                source.limit(endPos);

                // write the size
                bytes.writeStopBit(source.remaining());

                while (source.remaining() > 0) {
                    if (source.remaining() >= 8)
                        bytes.writeLong(source.readLong());
                    else
                        bytes.writeByte(source.readByte());
                }

            } finally {
                wire.bytes().position(endPos);
                wire.bytes().limit(limit);
            }

        }

        return bytes.flip();
    }


    @Nullable
    private Work sendException(@NotNull Wire wire, @NotNull Throwable e) {
        final StringWriter sw = new StringWriter();
        final PrintWriter pw = new PrintWriter(sw);
        e.printStackTrace(pw);
        outWire.write(() -> "IS_EXCEPTION").bool(true);
        wire.write(() -> "EXCEPTION").text(sw.toString());
        nofityDataWritten();
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
                ? bytesChronicleMaps.get(channelId)
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
    private Work writeValue(Consumer<BytesChronicleMap> process) {

        final BytesChronicleMap bytesMap = bytesMap(channelId);

        inLanByteBuffer.clear();
        inLangBytes.clear();
        outMessageAdapter.markStartOfMessage();
        bytesMap.output = outMessageAdapter.outBuffer;

        try {
            process.accept(bytesMap);
        } catch (Exception e) {


            // the idea of wire is that is platform independent,
            // so we wil have to send the exception as a String
            outWire.write(() -> "IS_EXCEPTION").bool(true);
            outWire.write(() -> "EXCEPTION").text(toString(e));
            LOG.error("", e);
            return null;
        }

        outMessageAdapter.accept(outWire);
        nofityDataWritten();
        return null;
    }


    @Nullable
    private Work write(Consumer<BytesChronicleMap> c) {

        final BytesChronicleMap bytesMap = bytesMap(channelId);
        bytesMap.output = null;

        long start = outWire.bytes().position();
        outWire.write(() -> "IS_EXCEPTION").bool(false);

        try {
            c.accept(bytesMap);
        } catch (Exception e) {
            outWire.bytes().position(start);
            // the idea of wire is that is platform independent,
            // so we wil have to send the exception as a String
            outWire.write(() -> "IS_EXCEPTION").bool(true);
            outWire.write(() -> "EXCEPTION").text(toString(e));
            LOG.error("", e);
            return null;
        }

        nofityDataWritten();
        return null;
    }

    private void showOutWire() {
        System.out.println("pos=" + outWire.bytes().position() + ",bytes=" + Bytes.toDebugString(outWire.bytes(), 0, outWire.bytes().position()));
    }

    @Nullable
    private Work writeVoid(Consumer<BytesChronicleMap> process) {

        // skip 4 bytes where we will write the size
        final long start = outWire.bytes().position();
        final BytesChronicleMap bytesMap = bytesMap(channelId);

        bytesMap.output = null;

        try {
            process.accept(bytesMap);
        } catch (Exception e) {

            LOG.error("", e);

            return null;
        }


        return null;
    }

    /**
     * converts the exception into a String, so that it can be sent to c# clients
     */
    private String toString(Throwable t) {
        final StringWriter sw = new StringWriter();
        final PrintWriter pw = new PrintWriter(sw);
        t.printStackTrace(pw);
        return sw.toString();
    }


    /**
     * used to send data to the wired stateless client, this class converts/adapts a chronicle map
     * lang-bytes output into chronicle-bytes
     */
    private static class OutMessageAdapter {

        private IByteBufferBytes outLangBytes = ByteBufferBytes.wrap(ByteBuffer.allocate(1024));
        private Bytes outChronBytes = Bytes.wrap(outLangBytes.buffer());

        // chronicle map will write its result in here
        final MapIOBuffer outBuffer = new MapIOBuffer() {

            @Override
            public void ensureBufferSize(long l) {

                if (outLangBytes.remaining() >= l)
                    return;

                long size = outLangBytes.capacity() + l;
                if (size > Integer.MAX_VALUE)
                    throw new BufferOverflowException();

                // record the current position and limit
                long position = outLangBytes.position();

                // create a new buffer and copy the data into it
                outLangBytes.clear();
                final IByteBufferBytes newOutLangBytes = ByteBufferBytes.wrap(ByteBuffer.allocate((int) size));
                newOutLangBytes.write(outLangBytes);

                outChronBytes = Bytes.wrap(outLangBytes.buffer());

                newOutLangBytes.limit(newOutLangBytes.capacity());
                newOutLangBytes.position(position);

                outChronBytes.limit(outChronBytes.capacity());
                outChronBytes.position(position);
            }

            @Override
            public net.openhft.lang.io.Bytes in() {
                return outLangBytes;
            }
        };
        private long startChunk;


        void clear() {
            outChronBytes.clear();
            outLangBytes.clear();
        }


        /**
         * adapts the chronicle out lang bytes to chroncile bytes
         *
         * @param wire the wire that we wish to append data to
         */
        void accept(Wire wire) {

            wire.write(() -> "IS_EXCEPTION").bool(false);

            // flips calls flip on this message so that we can read it
            flipMessage();

            // set the chron-bytes and the lang-bytes to be the same
            outChronBytes.position(outLangBytes.position());
            outChronBytes.limit(outLangBytes.limit());

            // is Null
            boolean isNull = outChronBytes.readBoolean();

            // read the size - not used
            long l = outChronBytes.readStopBit();

            wire.write(() -> "RESULT_IS_NULL").bool(isNull);
            if (!isNull) {

                // write the result
                wire.write(() -> "RESULT");
                System.out.println("out->" + Bytes.toDebugString(outChronBytes));
                wire.bytes().write(outChronBytes);
            }
        }

        private void flipMessage() {
            long position = outLangBytes.position();
            outLangBytes.position(startChunk);
            outLangBytes.limit(position);
        }

        /**
         * marks the start of the message
         */
        public void markStartOfMessage() {
            startChunk = outLangBytes.position();
            System.out.println("Start == " + startChunk);
        }

    }


    private void nofityDataWritten() {
        key.interestOps(OP_WRITE | OP_READ);
    }


}
