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
import net.openhft.chronicle.hash.impl.util.BuildVersion;
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
import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;

import static java.nio.channels.SelectionKey.OP_READ;
import static java.nio.channels.SelectionKey.OP_WRITE;

/**
 * @author Rob Austin.
 */
class StatelessWiredConnector<K extends BytesMarshallable, V extends BytesMarshallable> {

    private static final Logger LOG = LoggerFactory.getLogger(StatelessWiredConnector.class);
    private final NativeBytes inLangBytes = new NativeBytes(0, 0);

    @NotNull
    private final OutMessageAdapter outMessageAdapter = new OutMessageAdapter();
    @NotNull
    private final ArrayList<BytesChronicleMap> bytesChronicleMaps = new ArrayList<>();

    private final TextWire inWire = new TextWire(Bytes.elasticByteBuffer());
    private final TextWire outWire = new TextWire(Bytes.elasticByteBuffer());
    //private final ByteBuffer resultBuffer = ByteBuffer.allocateDirect(64);
    @NotNull
    private final ByteBuffer inLanByteBuffer = ByteBuffer.allocateDirect(64);
    private final StringBuilder methodName = new StringBuilder();
    private final byte localIdentifier;

    private long timestamp;
    private short channelId;

    @NotNull
    private final List<Replica> channelList;

    final Consumer writeElement = new Consumer<Iterator<byte[]>>() {

        @Override
        public void accept(Iterator<byte[]> iterator) {
            outWire.write(() -> "RESULT");
            outWire.bytes().write(iterator.next());
        }
    };

    final Consumer writeEntry = new Consumer<Iterator<Map.Entry<byte[], byte[]>>>() {

        @Override
        public void accept(Iterator<Map.Entry<byte[], byte[]>> iterator) {

            final Map.Entry<byte[], byte[]> entry = iterator.next();

            outWire.write(() -> "RESULT_KEY");
            outWire.bytes().write(entry.getKey());

            outWire.write(() -> "RESULT_VALUE");
            outWire.bytes().write(entry.getValue());
        }
    };

    private Runnable in = null;

    private byte remoteIdentifier;
    private byte localIdentifer;
    private Runnable out = null;

    public StatelessWiredConnector(@Nullable List<Replica> channelList, byte localIdentifier) {

        if (channelList == null) {
            throw new IllegalStateException("The StatelessWiredConnector currently only support maps " +
                    "that are set up via the replication channel. localIdentifier=" + localIdentifier);
        }
        this.channelList = channelList;
        this.localIdentifier = localIdentifier;

    }

    private SelectionKey key;

    public void onWrite(@NotNull SocketChannel socketChannel, @NotNull SelectionKey key) throws IOException {

        if (out != null) {

            long markStart = outWire.bytes().position();
            outWire.bytes().skip(4);

            try {
                out.run();
            } finally {
                int len = (int) (outWire.bytes().position() - markStart);
                outWire.bytes().writeInt(markStart, len);
            }

        }

        long position = outWire.bytes().position();

        if (position > Integer.MAX_VALUE)
            throw new BufferOverflowException();

        ByteBuffer buffer = (ByteBuffer) outWire.bytes().underlyingObject();
        buffer.limit((int) position);

        socketChannel.write(buffer);


        if (buffer.remaining() == 0) {
            buffer.clear();
            outWire.bytes().clear();
            if (out == null)
                key.interestOps(OP_READ);
            outMessageAdapter.clear();
        }

    }

    public int onRead(@NotNull SocketChannel socketChannel, SelectionKey key) throws IOException {


        this.key = key;

        final int len = readSocket(socketChannel);

        if (len == -1 || len == 0)
            return len;


        while (inWire.bytes().remaining() > 4) {

            final long limit = inWire.bytes().limit();

            if (nextWireMessage() == null) {
                if (shouldCompactInBuffer())
                    compactInBuffer();
                else if (shouldClearInBuffer())
                    clearInBuffer();
                return len;
            }

            long nextPosition = inWire.bytes().limit();
            try {
                onEvent();
            } finally {
                inWire.bytes().position(nextPosition);
                inWire.bytes().limit(limit);
            }
        }
        return len;
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
    private boolean shouldCompactInBuffer() {
        return inWire.bytes().position() > 0 && inWire.bytes().remaining() < (inWireBuffer().capacity() / 2);
    }


    private void compactInBuffer() {
        inWireBuffer().position((int) inWire.bytes().position());
        inWireBuffer().limit(inWireBuffer().position());
        inWireBuffer().compact();

        inWire.bytes().position(0);
        inWire.bytes().limit(0);
    }

    @NotNull
    private ByteBuffer inWireBuffer() {
        return (ByteBuffer) inWire.bytes().underlyingObject();
    }

    private int readSocket(@NotNull SocketChannel socketChannel) throws IOException {
        ByteBuffer dst = inWireBuffer();
        int len = socketChannel.read(dst);
        int readUpTo = dst.position();
        inWire.bytes().limit(readUpTo);
        return len;
    }

    @Nullable
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


    Map<Long, Runnable> incompleteWork = new HashMap<>();


    private void writeChunked(long transactionId,
                              @NotNull final Function<Map, Iterator<byte[]>> function,
                              @NotNull final Consumer<Iterator<byte[]>> c) {

        final BytesChronicleMap m = bytesMap(channelId);
        final Iterator<byte[]> iterator = function.apply(m);

        // this allows us to write more data than the buffer will allow
        out = () -> {

            // each chunk has its own transaction-id
            outWire.write(() -> "TRANSACTION_ID").int64(transactionId);

            write(map -> {

                boolean hasNext = iterator.hasNext();
                outWire.write(() -> "HAS_NEXT").bool(hasNext);

                if (hasNext)
                    c.accept(iterator);
                else
                    // setting out to NULL denotes that there are no more chunks
                    out = null;
            });

        };

        out.run();

    }

    @SuppressWarnings("UnusedReturnValue")
    void onEvent() {

        // it is assumed by this point that the buffer has all the bytes in it for this message
        long transactionId = inWire.read(() -> "TRANSACTION_ID").int64();
        timestamp = inWire.read(() -> "TIME_STAMP").int64();
        channelId = inWire.read(() -> "CHANNEL_ID").int16();
        inWire.read(() -> "METHOD_NAME").text(methodName);

        if ("PUT_WITHOUT_ACC".contentEquals(methodName)) {
            writeVoid(bytesMap -> {
                final net.openhft.lang.io.Bytes reader = toReader(inWire, "ARG_1", "ARG_2");
                bytesMap.put(reader, reader, timestamp, identifier());
            });
            return;
        }

        // for the length
        long markStart = outWire.bytes().position();
        outWire.bytes().skip(4);

        try {


            if ("KEY_SET".contentEquals(methodName)) {
                writeChunked(transactionId, map -> map.keySet().iterator(), writeElement);
                return;
            }

            if ("VALUES".contentEquals(methodName)) {
                writeChunked(transactionId, map -> map.values().iterator(), writeElement);
                return;
            }

            if ("ENTRY_SET".contentEquals(methodName)) {
                writeChunked(transactionId, map -> map.entrySet().iterator(), writeEntry);
                return;
            }

            if ("PUT_ALL".contentEquals(methodName)) {
                putAll(transactionId);
                return;
            }


            // write the transaction id
            outWire.write(() -> "TRANSACTION_ID").int64(transactionId);

            if ("REMOTE_IDENTIFIER".contentEquals(methodName)) {
                this.remoteIdentifier = inWire.read(() -> "RESULT").int8();
                return;
            }

            if ("LONG_SIZE".contentEquals(methodName)) {
                write(b -> outWire.write(() -> "RESULT").int64(b.longSize()));
                return;
            }

            if ("IS_EMPTY".contentEquals(methodName)) {
                write(b -> outWire.write(() -> "RESULT").bool(b.isEmpty()));
                return;
            }

            if ("CONTAINS_KEY".contentEquals(methodName)) {
                write(b -> outWire.write(() -> "RESULT").
                        bool(b.containsKey(toReader(inWire, "ARG_1"))));
                return;
            }

            if ("CONTAINS_VALUE".contentEquals(methodName)) {
                write(b -> outWire.write(() -> "RESULT").
                        bool(b.containsKey(toReader(inWire, "ARG_1"))));
                return;
            }

            if ("GET".contentEquals(methodName)) {
                writeValue(b -> b.get(toReader(inWire, "ARG_1")));
                return;
            }

            if ("PUT".contentEquals(methodName)) {
                writeValue(bytesMap -> {
                    final net.openhft.lang.io.Bytes reader = toReader(inWire, "ARG_1", "ARG_2");
                    bytesMap.put(reader, reader, timestamp, identifier());
                });
                return;
            }

            if ("REMOVE".contentEquals(methodName)) {
                writeValue(b -> b.remove(toReader(inWire, "ARG_1")));
                return;
            }

            if ("CLEAR".contentEquals(methodName)) {
                writeVoid(BytesChronicleMap::clear);
                return;
            }

            if ("REPLACE".contentEquals(methodName)) {
                writeValue(bytesMap -> {
                    final net.openhft.lang.io.Bytes reader = toReader(inWire, "ARG_1", "ARG_2");
                    bytesMap.replace(reader, reader);
                });
                return;
            }

            if ("REPLACE_WITH_OLD_AND_NEW_VALUE".contentEquals(methodName)) {
                writeValue(bytesMap -> {
                    final net.openhft.lang.io.Bytes reader = toReader(inWire, "ARG_1", "ARG_2", "ARG_3");
                    bytesMap.replace(reader, reader, reader);
                });
                return;
            }

            if ("PUT_IF_ABSENT".contentEquals(methodName)) {
                writeValue(bytesMap -> {
                    final net.openhft.lang.io.Bytes reader = toReader(inWire, "ARG_1", "ARG_2");
                    bytesMap.putIfAbsent(reader, reader, timestamp, identifier());
                });
                return;
            }

            if ("REMOVE_WITH_VALUE".contentEquals(methodName)) {
                writeValue(bytesMap -> {
                    final net.openhft.lang.io.Bytes reader = toReader(inWire, "ARG_1", "ARG_2");
                    bytesMap.remove(reader, reader, timestamp, identifier());
                });
                return;
            }

            if ("TO_STRING".contentEquals(methodName)) {
                write(b -> outWire.write(() -> "RESULT").text(b.toString()));

                return;
            }

            if ("APPLICATION_VERSION".contentEquals(methodName)) {
                write(b -> outWire.write(() -> "RESULT").text(applicationVersion()));
                return;
            }

            if ("PERSISTED_DATA_VERSION".contentEquals(methodName)) {
                write(b -> outWire.write(() -> "RESULT").text(persistedDataVersion()));

                return;
            }

            if ("HASH_CODE".contentEquals(methodName)) {
                write(b -> outWire.write(() -> "RESULT").int32(b.hashCode()));
                return;
            }


            throw new IllegalStateException("unsupported event=" + methodName);

            //  else if ("MAP_FOR_KEY".contentEquals(methodName)
            // else if ("PUT_MAPPED".contentEquals(methodName))
            //  else if ("VALUE_BUILDER".contentEquals(methodName))


        } finally {


            int len = (int) (outWire.bytes().position() - markStart);

            if (len == 4)
                // remove the 4 byte len as nothing was written
                outWire.bytes().position(markStart);
            else
                outWire.bytes().writeInt(markStart, len);

            if (len > 4)
                System.out.println("\n----------------------------\n\nserver wrote:\n" + Bytes.toDebugString(outWire.bytes(), markStart + 4, len - 4));
        }


    }

    private byte[] toBytes(String fieldName) {

        Wire wire = inWire;
        ValueIn read = wire.read(() -> fieldName);

        long l = read.readLength();
        if (l > Integer.MAX_VALUE)
            throw new BufferOverflowException();

        int fieldLength = (int) l;

        long endPos = wire.bytes().position() + fieldLength;
        long limit = wire.bytes().limit();

        try {
            byte[] bytes = new byte[fieldLength];

            wire.bytes().read(bytes);
            return bytes;

        } finally

        {
            wire.bytes().position(endPos);
            wire.bytes().limit(limit);
        }
    }

    private void putAll(long transactionId) {

        final BytesChronicleMap bytesMap = bytesMap(StatelessWiredConnector.this.channelId);

        if (bytesMap == null)
            return;

        Runnable runnable = incompleteWork.get(transactionId);

        if (runnable != null) {
            runnable.run();
            return;
        }

        runnable = new Runnable() {

            // we should try and collect the data and then apply it atomically as quickly possible
            final Map<byte[], byte[]> collectData = new HashMap<>();

            @Override
            public void run() {
                if (inWire.read(() -> "HAS_NEXT").bool()) {
                    collectData.put(toBytes("ARG_1"), toBytes("ARG_2"));
                } else {
                    // the old code assumed that all the data would fit into a single buffer
                    // this assumption is invalid
                    if (!collectData.isEmpty()) {
                        bytesMap.delegate.putAll((Map) collectData);
                        incompleteWork.remove(transactionId);
                        outWire.write(() -> "TRANSACTION_ID").int64(transactionId);

                        // todo handle the case where there is an exception
                        outWire.write(() -> "IS_EXCEPTION").bool(false);

                        nofityDataWritten();
                    }
                }
            }
        };

        incompleteWork.put(transactionId, runnable);
        runnable.run();

    }


    private byte identifier() {
        // if the client provides a remote identifier then we will use that otherwise we will use the local identifier
        return remoteIdentifier == 0 ? localIdentifer : remoteIdentifier;
    }

    @NotNull
    private CharSequence persistedDataVersion() {
        BytesChronicleMap bytesChronicleMap = bytesMap(channelId);
        if (bytesChronicleMap == null)
            return "";
        return bytesChronicleMap.persistedDataVersion();
    }

    @NotNull
    private CharSequence applicationVersion() {
        return BuildVersion.version();
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


    /**
     * gets the map for this channel id
     *
     * @param channelId the ID of the map
     * @return the chronicle map with this {@code channelId}
     */
    @NotNull
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
    @Nullable
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


    @SuppressWarnings("SameReturnValue")
    private void writeValue(@NotNull Consumer<BytesChronicleMap> process) {

        final BytesChronicleMap bytesMap = bytesMap(channelId);

        if (bytesMap == null) {
            LOG.error("no map for channelId=" + channelId + " can be found.");
            return;
        }

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
            return;
        }

        outMessageAdapter.accept(outWire);
        nofityDataWritten();

    }


    @SuppressWarnings("SameReturnValue")
    private void write(@NotNull Consumer<BytesChronicleMap> c) {

        final BytesChronicleMap bytesMap = bytesMap(channelId);

        if (bytesMap == null) {
            LOG.error("no map for channelId=" + channelId + " can be found.");
            return;
        }

        bytesMap.output = null;
        outWire.bytes().mark();
        outWire.write(() -> "IS_EXCEPTION").bool(false);

        try {
            c.accept(bytesMap);
        } catch (Exception e) {
            outWire.bytes().reset();
            // the idea of wire is that is platform independent,
            // so we wil have to send the exception as a String
            outWire.write(() -> "IS_EXCEPTION").bool(true);
            outWire.write(() -> "EXCEPTION").text(toString(e));
            LOG.error("", e);
            return;
        }

        nofityDataWritten();

    }

    /**
     * only used for debugging
     */
    @SuppressWarnings("UnusedDeclaration")
    private void showOutWire() {
        System.out.println("pos=" + outWire.bytes().position() + ",bytes=" + Bytes.toDebugString(outWire.bytes(), 0, outWire.bytes().position()));
    }

    @SuppressWarnings("SameReturnValue")
    private void writeVoid(@NotNull Consumer<BytesChronicleMap> process) {

        // skip 4 bytes where we will write the size
        final BytesChronicleMap bytesMap = bytesMap(channelId);

        if (bytesMap == null) {
            LOG.error("no map for channelId=" + channelId + " can be found.");
            return;
        }

        bytesMap.output = null;
        try {
            process.accept(bytesMap);
        } catch (Exception e) {
            LOG.error("", e);
            return;
        }


    }

    /**
     * converts the exception into a String, so that it can be sent to c# clients
     */
    private String toString(@NotNull Throwable t) {
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

        @NotNull
        private final IByteBufferBytes outLangBytes = ByteBufferBytes.wrap(ByteBuffer.allocate(1024));
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

            @NotNull
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
        void accept(@NotNull Wire wire) {

            wire.write(() -> "IS_EXCEPTION").bool(false);

            // flips calls flip on this message so that we can read it
            flipMessage();

            // set the chron-bytes and the lang-bytes to be the same
            outChronBytes.position(outLangBytes.position());
            outChronBytes.limit(outLangBytes.limit());

            // is Null
            boolean isNull = outChronBytes.readBoolean();

            // read the size - not used
            outChronBytes.readStopBit();

            wire.write(() -> "RESULT_IS_NULL").bool(isNull);
            if (!isNull) {
                // write the result
                wire.write(() -> "RESULT");
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
        }

    }

    private void nofityDataWritten() {
        key.interestOps(OP_WRITE | OP_READ);
    }


}
