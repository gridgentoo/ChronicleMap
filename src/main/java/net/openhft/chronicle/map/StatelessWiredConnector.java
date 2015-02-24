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

import net.openhft.chronicle.hash.function.SerializableFunction;
import net.openhft.chronicle.hash.serialization.BytesReader;
import net.openhft.chronicle.hash.serialization.internal.ReaderWithSize;
import net.openhft.chronicle.hash.serialization.internal.SerializationBuilder;
import net.openhft.chronicle.wire.TextWire;
import net.openhft.chronicle.wire.ValueIn;
import net.openhft.lang.io.AbstractBytes;
import net.openhft.lang.io.ByteBufferBytes;
import net.openhft.lang.io.Bytes;
import net.openhft.lang.model.DataValueClasses;
import net.openhft.lang.model.DataValueGenerator;
import net.openhft.lang.threadlocal.ThreadLocalCopies;
import net.openhft.lang.values.LongValue;
import net.openhft.xstream.converters.DataValueConverter;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.LongConsumer;

import static net.openhft.chronicle.map.AbstractChannelReplicator.SIZE_OF_SIZE;
import static net.openhft.chronicle.map.AbstractChannelReplicator.SIZE_OF_TRANSACTION_ID;

/**
 * @author Rob Austin.
 */
class StatelessWiredConnector<K, V> {

    public static final StatelessChronicleMap.EventId[] VALUES
            = StatelessChronicleMap.EventId.values();
    public static final int SIZE_OF_IS_EXCEPTION = 1;
    public static final int HEADER_SIZE = SIZE_OF_SIZE + SIZE_OF_IS_EXCEPTION +
            SIZE_OF_TRANSACTION_ID;
    private static final Logger LOG = LoggerFactory.getLogger(StatelessServerConnector.class
            .getName());
    @NotNull
    private final ReaderWithSize<K> keyReaderWithSize;

    @NotNull
    private final WriterWithSize<K> keyWriterWithSize;

    @NotNull
    private final ReaderWithSize<V> valueReaderWithSize;

    @NotNull
    private final WriterWithSize<V> valueWriterWithSize;

    @NotNull
    private final VanillaChronicleMap<K, ?, ?, V, ?, ?> map;
    private final BytesChronicleMap bytesMap;
    private final SerializationBuilder<K> keySerializationBuilder;
    private final SerializationBuilder<V> valueSerializationBuilder;
    private final int tcpBufferSize;


    StatelessWiredConnector(
            @NotNull VanillaChronicleMap<K, ?, ?, V, ?, ?> map,
            @NotNull final BufferResizer bufferResizer, int tcpBufferSize,
            final SerializationBuilder<K> keySerializationBuilder,
            final SerializationBuilder<V> valueSerializationBuilder) {
        this.tcpBufferSize = tcpBufferSize;

        this.keySerializationBuilder = keySerializationBuilder;
        this.valueSerializationBuilder = valueSerializationBuilder;
        keyReaderWithSize = new ReaderWithSize<>(keySerializationBuilder);
        keyWriterWithSize = new WriterWithSize<>(keySerializationBuilder, bufferResizer);
        valueReaderWithSize = new ReaderWithSize<>(valueSerializationBuilder);
        valueWriterWithSize = new WriterWithSize<>(valueSerializationBuilder, bufferResizer);
        this.map = map;
        bytesMap = new BytesChronicleMap(map);
    }

    @Nullable
    Work processStatelessEvent(final TextWire textWire,
                               @NotNull final TcpReplicator.TcpSocketChannelEntryWriter writer,
                               @NotNull final ByteBufferBytes reader) {


        final String methodName = textWire.read(() -> "METHOD_NAME").text();

        long transactionId;

        ValueIn read = textWire.read(() -> "TRANSACTION_ID");

        LongValue longValue = DataValueClasses.newDirectInstance(LongValue.class);
        textWire.read(() -> "TRANSACTION_ID").int64(longValue);
        long transacationId = longValue.getValue();


        final String identifier = textWire.read(() -> "IDENTIFIER").text();
        final String timestamp = textWire.read(() -> "TIMESTAMP").text();


        // these methods don't return a result to the client or don't return a result to the
        // client immediately
        switch (methodName) {
            case "KEY_SET":
                return keySet(reader, writer, transactionId);

            case "VALUES":
                return values(reader, writer, transactionId);

            case "ENTRY_SET":
                return entrySet(reader, writer, transactionId);

            case "PUT_WITHOUT_ACC":
                return put(reader, timestamp, identifier);

            case "PUT_ALL_WITHOUT_ACC":
                return putAll(reader, timestamp, identifier);

            case "REMOVE_WITHOUT_ACC":
                return remove(reader, timestamp, identifier);
        }

        final long sizeLocation = reflectTransactionId(writer.in(), transactionId);


        // these methods return a result

        switch (methodName) {
            case "LONG_SIZE":
                return longSize(writer, sizeLocation);

            case "IS_EMPTY":
                return isEmpty(writer, sizeLocation);

            case "CONTAINS_KEY":
                return containsKey(reader, writer, sizeLocation);

            case "CONTAINS_VALUE":
                return containsValue(reader, writer, sizeLocation);

            case "GET":
                return get(reader, writer, sizeLocation, timestamp);

            case "PUT":
                return put(reader, writer, sizeLocation, timestamp, identifier);

            case "REMOVE":
                return remove(reader, writer, sizeLocation, timestamp, identifier);

            case "CLEAR":
                return clear(writer, sizeLocation, timestamp, identifier);

            case "REPLACE":
                return replace(reader, writer, sizeLocation, timestamp, identifier);

            case "REPLACE_WITH_OLD_AND_NEW_VALUE":
                return replaceWithOldAndNew(reader, writer,
                        sizeLocation, timestamp, identifier);

            case "PUT_IF_ABSENT":
                return putIfAbsent(reader, writer, sizeLocation, timestamp, identifier);

            case "REMOVE_WITH_VALUE":
                return removeWithValue(reader, writer, sizeLocation, timestamp, identifier);

            case "TO_STRING":
                return toString(writer, sizeLocation);

            case "APPLICATION_VERSION":
                return applicationVersion(writer, sizeLocation);

            case "PERSISTED_DATA_VERSION":
                return persistedDataVersion(writer, sizeLocation);

            case "PUT_ALL":
                return putAll(reader, writer, sizeLocation, timestamp, identifier);

            case "HASH_CODE":
                return hashCode(writer, sizeLocation);

            case "MAP_FOR_KEY":
                return mapForKey(reader, writer, sizeLocation);

            case "PUT_MAPPED":
                return putMapped(reader, writer, sizeLocation);

            case "KEY_BUILDER":
                return writeBuilder(writer, sizeLocation, keySerializationBuilder);

            case "VALUE_BUILDER":
                return writeBuilder(writer, sizeLocation, valueSerializationBuilder);

            default:
                throw new IllegalStateException("unsupported event=" + event);
        }
    }

    private void writeObject(TcpReplicator.TcpSocketChannelEntryWriter writer, Object o) {
        for (; ; ) {
            long position = writer.in().position();

            try {
                writer.in().writeObject(o);
                return;
            } catch (IllegalStateException e) {
                if (e.getMessage().contains("Not enough available space")) {
                    writer.resizeToMessage(e);
                    writer.in().position(position);
                } else
                    throw e;
            }
        }
    }

    private Work writeBuilder(TcpReplicator.TcpSocketChannelEntryWriter writer,
                              long sizeLocation, SerializationBuilder builder) {

        try {
            writeObject(writer, builder);
        } catch (Exception e) {
            LOG.info("", e);

            return sendException(writer, sizeLocation, e);
        }

        writeSizeAndFlags(sizeLocation, false, writer.in());
        return null;
    }


    @Nullable
    public Work mapForKey(@NotNull ByteBufferBytes reader,
                          @NotNull TcpReplicator.TcpSocketChannelEntryWriter writer,
                          long sizeLocation) {
        final K key = keyReaderWithSize.read(reader, null, null);
        final SerializableFunction<V, ?> function = (SerializableFunction<V, ?>) reader.readObject();
        try {
            Object result = map.getMapped(key, function);
            writeObject(writer, result);
        } catch (Throwable e) {
            LOG.info("", e);
            return sendException(writer, sizeLocation, e);
        }

        writeSizeAndFlags(sizeLocation, false, writer.in());
        return null;
    }

    @Nullable
    public Work putMapped(@NotNull ByteBufferBytes reader,
                          @NotNull TcpReplicator.TcpSocketChannelEntryWriter writer,
                          long sizeLocation) {
        final K key = keyReaderWithSize.read(reader, null, null);
        final UnaryOperator<V> unaryOperator = (UnaryOperator<V>) reader.readObject();
        try {
            Object result = map.putMapped(key, unaryOperator);
            writeObject(writer, result);
        } catch (Throwable e) {
            LOG.info("", e);
            return sendException(writer, sizeLocation, e);
        }

        writeSizeAndFlags(sizeLocation, false, writer.in());
        return null;
    }

    @Nullable
    private Work removeWithValue(Bytes reader,
                                 @NotNull TcpReplicator.TcpSocketChannelEntryWriter writer,
                                 final long sizeLocation, long timestamp, byte id) {
        try {
            writer.ensureBufferSize(1L);
            writer.in().writeBoolean(bytesMap.remove(reader, reader));
        } catch (Throwable e) {
            return sendException(writer, sizeLocation, e);
        }
        writeSizeAndFlags(sizeLocation, false, writer.in());
        return null;
    }

    @Nullable
    private Work replaceWithOldAndNew(Bytes reader,
                                      @NotNull TcpReplicator.TcpSocketChannelEntryWriter writer,
                                      final long sizeLocation, long timestamp, byte id) {
        try {
            writer.ensureBufferSize(1L);
            writer.in().writeBoolean(bytesMap.replace(reader, reader, reader));
        } catch (Throwable e) {
            return sendException(writer, sizeLocation, e);
        }
        writeSizeAndFlags(sizeLocation, false, writer.in());
        return null;
    }

    @Nullable
    private Work longSize(@NotNull TcpReplicator.TcpSocketChannelEntryWriter writer,
                          final long sizeLocation) {
        try {
            writer.in().writeLong(map.longSize());
        } catch (Throwable e) {
            return sendException(writer, sizeLocation, e);
        }
        writeSizeAndFlags(sizeLocation, false, writer.in());
        return null;
    }

    @Nullable
    private Work hashCode(@NotNull TcpReplicator.TcpSocketChannelEntryWriter writer,
                          final long sizeLocation) {
        try {
            writer.in().writeInt(map.hashCode());
        } catch (Throwable e) {
            return sendException(writer, sizeLocation, e);
        }

        writeSizeAndFlags(sizeLocation, false, writer.in());
        return null;
    }

    @Nullable
    private Work toString(@NotNull TcpReplicator.TcpSocketChannelEntryWriter writer,
                          final long sizeLocation) {
        final String str;

        final long remaining = writer.in().remaining();
        try {
            str = map.toString();
        } catch (Throwable e) {
            return sendException(writer, sizeLocation, e);
        }

        assert remaining > 4;

        // this stops the toString overflowing the buffer
        final String result = (str.length() < remaining) ?
                str :
                str.substring(0, (int) (remaining - 4)) + "...";

        writeObject(writer, result);
        writeSizeAndFlags(sizeLocation, false, writer.in());
        return null;
    }


    @Nullable
    private Work applicationVersion(@NotNull TcpReplicator.TcpSocketChannelEntryWriter writer,
                                    final long sizeLocation) {


        final long remaining = writer.in().remaining();
        try {
            String result = map.applicationVersion();
            writeObject(writer, result);
        } catch (Throwable e) {
            return sendException(writer, sizeLocation, e);
        }

        assert remaining > 4;

        writeSizeAndFlags(sizeLocation, false, writer.in());
        return null;
    }


    @Nullable
    private Work persistedDataVersion(@NotNull TcpReplicator.TcpSocketChannelEntryWriter writer,
                                      final long sizeLocation) {
        final long remaining = writer.in().remaining();
        try {
            String result = map.persistedDataVersion();
            writeObject(writer, result);
        } catch (Throwable e) {
            return sendException(writer, sizeLocation, e);
        }

        assert remaining > 4;

        writeSizeAndFlags(sizeLocation, false, writer.in());
        return null;
    }

    @SuppressWarnings("SameReturnValue")
    @Nullable
    private Work sendException(@NotNull TcpReplicator.TcpSocketChannelEntryWriter writer,
                               long sizeLocation, @NotNull Throwable e) {
        // move the position to ignore any bytes written so far
        writer.in().position(sizeLocation + HEADER_SIZE);

        writeException(writer, e);

        writeSizeAndFlags(sizeLocation + SIZE_OF_TRANSACTION_ID, true, writer.in());
        return null;
    }

    @Nullable
    private Work isEmpty(@NotNull TcpReplicator.TcpSocketChannelEntryWriter writer,
                         final long sizeLocation) {
        try {
            writer.in().writeBoolean(map.isEmpty());
        } catch (Throwable e) {
            return sendException(writer, sizeLocation, e);
        }

        writeSizeAndFlags(sizeLocation, false, writer.in());
        return null;
    }

    @Nullable
    private Work containsKey(Bytes reader,
                             @NotNull TcpReplicator.TcpSocketChannelEntryWriter writer,
                             final long sizeLocation) {
        try {
            writer.in().writeBoolean(bytesMap.containsKey(reader));
        } catch (Throwable e) {
            return sendException(writer, sizeLocation, e);
        }
        writeSizeAndFlags(sizeLocation, false, writer.in());
        return null;
    }

    @Nullable
    private Work containsValue(Bytes reader,
                               @NotNull TcpReplicator.TcpSocketChannelEntryWriter writer,
                               final long sizeLocation) {
        // todo optimize -- eliminate
        final V v = valueReaderWithSize.read(reader, null, null);

        try {
            writer.in().writeBoolean(map.containsValue(v));
        } catch (Throwable e) {
            return sendException(writer, sizeLocation, e);
        }
        writeSizeAndFlags(sizeLocation, false, writer.in());
        return null;
    }

    @Nullable
    private Work get(Bytes reader,
                     TcpReplicator.TcpSocketChannelEntryWriter writer,
                     final long sizeLocation, long transactionId) {
        bytesMap.output = writer;
        try {
            bytesMap.get(reader);
        } catch (Throwable e) {
            return sendException(writer, sizeLocation, e);
        } finally {
            bytesMap.output = null;
        }
        writeSizeAndFlags(sizeLocation, false, writer.in());
        return null;
    }

    @SuppressWarnings("SameReturnValue")
    @Nullable
    private Work put(Bytes reader, long timestamp, byte id) {
        bytesMap.put(reader, reader);
        return null;
    }

    @Nullable
    private Work put(Bytes reader, TcpReplicator.TcpSocketChannelEntryWriter writer,
                     final long sizeLocation, long timestamp, byte id) {
        bytesMap.output = writer;
        try {
            bytesMap.put(reader, reader);
        } catch (Throwable e) {
            return sendException(writer, sizeLocation, e);
        } finally {
            bytesMap.output = null;
        }
        writeSizeAndFlags(sizeLocation, false, writer.in());
        return null;
    }

    @SuppressWarnings("SameReturnValue")
    @Nullable
    private Work remove(Bytes reader, long timestamp, byte id) {
        bytesMap.remove(reader);
        return null;
    }

    @Nullable
    private Work remove(Bytes reader, TcpReplicator.TcpSocketChannelEntryWriter writer,
                        final long sizeLocation, long timestamp, byte id) {
        bytesMap.output = writer;
        try {
            bytesMap.remove(reader);
        } catch (Throwable e) {
            return sendException(writer, sizeLocation, e);
        } finally {
            bytesMap.output = null;
        }
        writeSizeAndFlags(sizeLocation, false, writer.in());
        return null;
    }

    @Nullable
    private Work putAll(@NotNull Bytes reader,
                        @NotNull TcpReplicator.TcpSocketChannelEntryWriter writer,
                        final long sizeLocation,
                        long timestamp, byte id) {
        try {
            bytesMap.putAll(reader);
        } catch (Throwable e) {
            return sendException(writer, sizeLocation, e);
        }
        writeSizeAndFlags(sizeLocation, false, writer.in());
        return null;
    }

    @SuppressWarnings("SameReturnValue")
    @Nullable
    private Work putAll(@NotNull Bytes reader, long timestamp, byte id) {
        bytesMap.putAll(reader);
        return null;
    }

    @Nullable
    private Work clear(@NotNull TcpReplicator.TcpSocketChannelEntryWriter writer,
                       final long sizeLocation, long timestamp, byte id) {
        try {
            map.clear();
        } catch (Throwable e) {
            return sendException(writer, sizeLocation, e);
        }

        writeSizeAndFlags(sizeLocation, false, writer.in());
        return null;
    }

    @Nullable
    private Work values(@NotNull Bytes reader,
                        @NotNull TcpReplicator.TcpSocketChannelEntryWriter writer,
                        final long transactionId) {

        Collection<V> values;

        try {
            values = map.values();
        } catch (Throwable e) {
            return sendException(reader, writer, e);
        }

        final Iterator<V> iterator = values.iterator();

        // this allows us to write more data than the buffer will allow
        return new Work() {
            @Override
            public boolean doWork(@NotNull Bytes out) {
                final long sizeLocation = header(out, transactionId);

                ThreadLocalCopies copies = valueWriterWithSize.getCopies(null);
                Object valueWriter = valueWriterWithSize.writerForLoop(copies);

                int count = 0;
                while (iterator.hasNext()) {
                    // we've filled up the buffer, so lets give another channel a chance to send
                    // some data, we don't know the max key size, we will use the entrySize instead
                    if (out.position() > tcpBufferSize) {
                        writeHeader(out, sizeLocation, count, true);
                        return false;
                    }

                    count++;

                    valueWriterWithSize.writeInLoop(out, iterator.next(), valueWriter, copies);
                }

                writeHeader(out, sizeLocation, count, false);
                return true;
            }
        };
    }

    @Nullable
    private Work keySet(@NotNull Bytes reader,
                        @NotNull final TcpReplicator.TcpSocketChannelEntryWriter writer,
                        final long transactionId) {

        Set<K> ks;

        try {
            ks = map.keySet();
        } catch (Throwable e) {
            return sendException(reader, writer, e);
        }

        final Iterator<K> iterator = ks.iterator();

        // this allows us to write more data than the buffer will allow
        return new Work() {
            @Override
            public boolean doWork(@NotNull Bytes out) {
                final long sizeLocation = header(out, transactionId);

                ThreadLocalCopies copies = keyWriterWithSize.getCopies(null);
                Object keyWriter = keyWriterWithSize.writerForLoop(copies);

                int count = 0;
                while (iterator.hasNext()) {
                    // we've filled up the buffer, so lets give another channel a chance to send
                    // some data, we don't know the max key size, we will use the entrySize instead
                    if (out.position() > tcpBufferSize) {
                        writeHeader(out, sizeLocation, count, true);
                        return false;
                    }

                    count++;
                    K key = iterator.next();
                    keyWriterWithSize.writeInLoop(out, key, keyWriter, copies);
                }

                writeHeader(out, sizeLocation, count, false);
                return true;
            }
        };
    }

    @Nullable
    private Work entrySet(@NotNull final Bytes reader,
                          @NotNull TcpReplicator.TcpSocketChannelEntryWriter writer,
                          final long transactionId) {

        final Set<Map.Entry<K, V>> entries;

        try {
            entries = map.entrySet();
        } catch (Throwable e) {
            return sendException(reader, writer, e);
        }

        final Iterator<Map.Entry<K, V>> iterator = entries.iterator();

        // this allows us to write more data than the buffer will allow
        return new Work() {
            @Override
            public boolean doWork(@NotNull Bytes out) {
                if (out.position() > tcpBufferSize)
                    return false;

                final long sizeLocation = header(out, transactionId);

                ThreadLocalCopies copies = keyWriterWithSize.getCopies(null);
                Object keyWriter = keyWriterWithSize.writerForLoop(copies);
                copies = valueWriterWithSize.getCopies(copies);
                Object valueWriter = valueWriterWithSize.writerForLoop(copies);

                int count = 0;
                while (iterator.hasNext()) {
                    // we've filled up the buffer, so lets give another channel a chance to send
                    // some data, we don't know the max key size, we will use the entrySize instead
                    if (out.position() > tcpBufferSize) {
                        writeHeader(out, sizeLocation, count, true);
                        return false;
                    }

                    count++;
                    final Map.Entry<K, V> next = iterator.next();
                    keyWriterWithSize.writeInLoop(out, next.getKey(), keyWriter, copies);
                    valueWriterWithSize.writeInLoop(out, next.getValue(), valueWriter, copies);
                }

                writeHeader(out, sizeLocation, count, false);
                return true;
            }
        };
    }

    @Nullable
    private Work putIfAbsent(Bytes reader, TcpReplicator.TcpSocketChannelEntryWriter writer,
                             final long sizeLocation, long timestamp, byte id) {
        bytesMap.output = writer;
        try {
            bytesMap.putIfAbsent(reader, reader);
        } catch (Throwable e) {
            return sendException(writer, sizeLocation, e);
        } finally {
            bytesMap.output = null;
        }
        writeSizeAndFlags(sizeLocation, false, writer.in());
        return null;
    }

    @Nullable
    private Work replace(Bytes reader, TcpReplicator.TcpSocketChannelEntryWriter writer,
                         final long sizeLocation, long timestamp, byte id) {
        bytesMap.output = writer;
        try {
            bytesMap.replace(reader, reader);
        } catch (Throwable e) {
            return sendException(writer, sizeLocation, e);
        } finally {
            bytesMap.output = null;
        }
        writeSizeAndFlags(sizeLocation, false, writer.in());
        return null;
    }

    private long reflectTransactionId(@NotNull Bytes writer, final long transactionId) {
        final long sizeLocation = writer.position();
        writer.skip(SIZE_OF_SIZE);
        assert transactionId != 0;
        writer.writeLong(transactionId);
        writer.writeBoolean(false); // isException
        return sizeLocation;
    }

    private void writeSizeAndFlags(long locationOfSize, boolean isException, @NotNull Bytes out) {
        final long size = out.position() - locationOfSize;

        out.writeInt(locationOfSize, (int) size); // size

        // write isException
        out.writeBoolean(locationOfSize + SIZE_OF_SIZE + SIZE_OF_TRANSACTION_ID, isException);

        long pos = out.position();
        long limit = out.limit();

        //   System.out.println("Sending with size=" + size);

        if (LOG.isDebugEnabled()) {
            out.position(locationOfSize);
            out.limit(pos);
            LOG.info("Sending to the stateless client, bytes=" + AbstractBytes.toHex(out) + "," +
                    "len=" + out.remaining());
            out.limit(limit);
            out.position(pos);
        }


    }

    private void writeException(@NotNull TcpReplicator.TcpSocketChannelEntryWriter out,
                                Throwable e) {
        writeObject(out, e);
    }

    @NotNull
    private Map<K, V> readEntries(@NotNull Bytes reader) {
        final long numberOfEntries = reader.readStopBit();
        final Map<K, V> result = new HashMap<K, V>();

        ThreadLocalCopies copies = keyReaderWithSize.getCopies(null);
        BytesReader<K> keyReader = keyReaderWithSize.readerForLoop(copies);
        copies = valueReaderWithSize.getCopies(copies);
        BytesReader<V> valueReader = valueReaderWithSize.readerForLoop(copies);

        for (long i = 0; i < numberOfEntries; i++) {
            K key = keyReaderWithSize.readInLoop(reader, keyReader);
            V value = valueReaderWithSize.readInLoop(reader, valueReader);
            result.put(key, value);
        }
        return result;
    }

    @Nullable
    private Work sendException(@NotNull Bytes reader,
                               @NotNull TcpReplicator.TcpSocketChannelEntryWriter writer,
                               @NotNull Throwable e) {
        final long sizeLocation = reflectTransactionId(writer.in(), reader.readLong());
        return sendException(writer, sizeLocation, e);
    }

    private long header(@NotNull Bytes writer, final long transactionId) {
        final long sizeLocation = writer.position();

        writer.skip(SIZE_OF_SIZE);
        writer.writeLong(transactionId);

        // exception
        writer.skip(1);

        //  hasAnotherChunk
        writer.skip(1);

        // count
        writer.skip(4);
        return sizeLocation;
    }

    private void writeHeader(@NotNull Bytes writer, long sizeLocation, int count,
                             final boolean hasAnotherChunk) {
        final long end = writer.position();
        final int size = (int) (end - sizeLocation);
        writer.position(sizeLocation);

        // size in bytes
        writer.writeInt(size);

        //transaction id;
        writer.skip(8);

        // is exception
        writer.writeBoolean(false);

        writer.writeBoolean(hasAnotherChunk);

        // count
        writer.writeInt(count);
        writer.position(end);
    }

}
