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

import net.openhft.chronicle.hash.function.SerializableFunction;
import net.openhft.chronicle.hash.serialization.BytesReader;
import net.openhft.chronicle.hash.serialization.internal.ReaderWithSize;
import net.openhft.chronicle.hash.serialization.internal.SerializationBuilder;
import net.openhft.lang.io.Bytes;
import net.openhft.lang.threadlocal.ThreadLocalCopies;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.function.Consumer;
import java.util.function.Predicate;

import static java.util.Collections.emptyList;
import static net.openhft.chronicle.map.StatelessChronicleMap.EventId.*;


/**
 * @author Rob Austin.
 */
class StatelessChronicleMap<K, V> implements ChronicleMap<K, V>, Cloneable {

    private static final Logger LOG = LoggerFactory.getLogger(StatelessChronicleMap.class);
    private final StatelessClientTcpConnectionHub hub;

    @NotNull
    protected ReaderWithSize<K> keyReaderWithSize;
    @NotNull
    protected WriterWithSize<K> keyWriterWithSize;
    @NotNull
    protected ReaderWithSize<V> valueReaderWithSize;
    @NotNull
    protected WriterWithSize<V> valueWriterWithSize;
    protected Class<K> kClass;
    protected Class<V> vClass;

    private boolean putReturnsNull;
    private boolean removeReturnsNull;

    StatelessChronicleMap(@NotNull final ChronicleMapStatelessClientBuilder config) {


        hub = config.hub;
        this.putReturnsNull = config.putReturnsNull();
        this.removeReturnsNull = config.removeReturnsNull();

        loadKeyValueSerializers();
    }

    protected void loadKeyValueSerializers() {
        final SerializationBuilder keyBuilder =
                hub.fetchObject(SerializationBuilder.class, KEY_BUILDER);
        kClass = keyBuilder.eClass;
        final SerializationBuilder valueBuilder =
                hub.fetchObject(SerializationBuilder.class, VALUE_BUILDER);
        vClass = valueBuilder.eClass;

        keyReaderWithSize = new ReaderWithSize(keyBuilder);
        keyWriterWithSize = new WriterWithSize(keyBuilder, hub.outBufferResizer());

        valueReaderWithSize = new ReaderWithSize(valueBuilder);
        valueWriterWithSize = new WriterWithSize(valueBuilder, hub.outBufferResizer());
    }

    @SuppressWarnings("UnusedDeclaration")
    void identifier(int identifier) {
        hub.identifier = identifier;
    }

    @Override
    public void getAll(File toFile) throws IOException {
        JsonSerializer.getAll(toFile, this, emptyList());
    }

    @Override
    public void putAll(File fromFile) throws IOException {
        JsonSerializer.putAll(fromFile, this, emptyList());
    }

    @Override
    public V newValueInstance() {
        if (vClass.equals(CharSequence.class) || vClass.equals(StringBuilder.class)) {
            return (V) new StringBuilder();
        }
        return VanillaChronicleMap.newInstance(vClass, false);
    }

    @Override
    public K newKeyInstance() {
        return VanillaChronicleMap.newInstance(kClass, true);
    }

    @Override
    public Class<K> keyClass() {
        return kClass;
    }

    @Override
    public boolean forEachEntryWhile(Predicate<? super MapKeyContext<K, V>> predicate) {
        // TODO implement!
        throw new UnsupportedOperationException();
    }

    @Override
    public void forEachEntry(Consumer<? super MapKeyContext<K, V>> action) {
        // TODO implement!
        throw new UnsupportedOperationException();
    }


    public String serverApplicationVersion() {
        return hub.serverApplicationVersion();
    }

    @Override
    public void close() {
        // todo add ref count
    }

    @Override
    public Class<V> valueClass() {
        return vClass;
    }


    @NotNull
    public File file() {
        throw new UnsupportedOperationException();
    }

    @SuppressWarnings("NullableProblems")
    public V putIfAbsent(K key, V value) {

        if (key == null || value == null)
            throw new NullPointerException();

        return fetchObject(vClass, PUT_IF_ABSENT, key, value);
    }

    @SuppressWarnings("NullableProblems")
    public boolean remove(Object key, Object value) {

        if (key == null)
            throw new NullPointerException();

        return value != null && fetchBoolean(REMOVE_WITH_VALUE, (K) key, (V) value);

    }

    @SuppressWarnings("NullableProblems")
    public boolean replace(K key, V oldValue, V newValue) {
        if (key == null || oldValue == null || newValue == null)
            throw new NullPointerException();

        return fetchBoolean(REPLACE_WITH_OLD_AND_NEW_VALUE, key, oldValue, newValue);
    }

    @SuppressWarnings("NullableProblems")
    public V replace(K key, V value) {
        if (key == null || value == null)
            throw new NullPointerException();

        return fetchObject(vClass, REPLACE, key, value);
    }

    public int size() {
        return (int) longSize();
    }

    /**
     * calling this method should be avoided at all cost, as the entire {@code object} is
     * serialized. This equals can be used to compare map that extends ChronicleMap.  So two
     * Chronicle Maps that contain the same data are considered equal, even if the instances of the
     * chronicle maps were of different types
     *
     * @param object the object that you are comparing against
     * @return true if the contain the same data
     */
    @Override
    public boolean equals(@Nullable Object object) {
        if (this == object) return true;
        if (object == null || object.getClass().isAssignableFrom(Map.class))
            return false;

        final Map<? extends K, ? extends V> that = (Map<? extends K, ? extends V>) object;

        final int size = size();

        if (that.size() != size)
            return false;

        final Set<Map.Entry<K, V>> entries = entrySet();
        return that.entrySet().equals(entries);
    }

    @Override
    public int hashCode() {
        return fetchInt(HASH_CODE);
    }

    @NotNull
    public String toString() {
        return hub.fetchObject(String.class, TO_STRING);
    }

    @NotNull
    public String serverPersistedDataVersion() {
        return hub.fetchObject(String.class, PERSISTED_DATA_VERSION);
    }

    public boolean isEmpty() {
        return fetchBoolean(IS_EMPTY);
    }

    public boolean containsKey(Object key) {
        return fetchBooleanK(CONTAINS_KEY, (K) key);
    }

    @NotNull
    private NullPointerException keyNotNullNPE() {
        return new NullPointerException("key can not be null");
    }

    public boolean containsValue(Object value) {
        return fetchBooleanV(CONTAINS_VALUE, (V) value);
    }

    public long longSize() {
        return fetchLong(LONG_SIZE);
    }

    @Override
    public MapKeyContext<K, V> context(K key) {
        throw new UnsupportedOperationException("Contexts are not supported by stateless clients");
    }

    public V get(Object key) {
        return fetchObject(vClass, GET, (K) key);
    }

    @Nullable
    public V getUsing(K key, V usingValue) {


        final long startTime = System.currentTimeMillis();
        ThreadLocalCopies copies;

        long transactionId;

        hub.outBytesLock().lock();
        try {

            final long sizeLocation = hub.writeEventAnSkip(GET);

            copies = writeKey((K) key);
            transactionId = hub.send(sizeLocation, startTime);
        } finally {
            hub.outBytesLock().unlock();
        }

        if (eventReturnsNull(GET))
            return null;

        return (V) readValue(transactionId, startTime, copies, usingValue);

    }

    @NotNull
    public V acquireUsing(@NotNull K key, V usingValue) {
        throw new UnsupportedOperationException("acquireUsing() is not supported for stateless " +
                "clients");
    }

    @NotNull
    @Override
    public MapKeyContext<K, V> acquireContext(@NotNull K key, @NotNull V usingValue) {
        throw new UnsupportedOperationException("Contexts are not supported by stateless clients");
    }

    public V remove(Object key) {
        if (key == null)
            throw keyNotNullNPE();
        return fetchObject(vClass, removeReturnsNull ? REMOVE_WITHOUT_ACC : REMOVE, (K) key);
    }

    public V put(K key, V value) {
        if (key == null || value == null)
            throw new NullPointerException();
        return fetchObject(vClass, putReturnsNull ? PUT_WITHOUT_ACC : PUT, key, value);
    }

    @Nullable
    public <R> R getMapped(@Nullable K key, @NotNull SerializableFunction<? super V, R> function) {
        if (key == null)
            throw keyNotNullNPE();
        return fetchObject(MAP_FOR_KEY, key, function);
    }

    @Nullable
    @Override
    public V putMapped(@Nullable K key, @NotNull UnaryOperator<V> unaryOperator) {
        if (key == null)
            throw keyNotNullNPE();
        return fetchObject(PUT_MAPPED, key, unaryOperator);
    }

    public void clear() {
        fetchVoid(CLEAR);
    }

    @NotNull
    public Collection<V> values() {

        final long timeoutTime;
        final long transactionId;

        hub.outBytesLock().lock();
        try {

            final long sizeLocation = hub.writeEventAnSkip(VALUES);
            final long startTime = System.currentTimeMillis();

            timeoutTime = System.currentTimeMillis() + hub.timeoutMs;
            transactionId = hub.send(sizeLocation, startTime);
        } finally {
            hub.outBytesLock().unlock();
        }

        // get the data back from the server
        final Collection<V> result = new ArrayList<V>();

        BytesReader<V> valueReader = valueReaderWithSize.readerForLoop(null);
        for (; ; ) {
            hub.inBytesLock().lock();
            try {

                final Bytes in = hub.blockingFetchReadOnly(timeoutTime, transactionId);

                final boolean hasMoreEntries = in.readBoolean();

                // number of entries in this chunk
                final long size = in.readInt();

                for (int i = 0; i < size; i++) {
                    result.add(valueReaderWithSize.readInLoop(in, valueReader));
                }

                if (!hasMoreEntries)
                    break;

            } finally {
                hub.inBytesLock().unlock();
            }
        }
        return result;
    }

    @NotNull
    public Set<Map.Entry<K, V>> entrySet() {
        final long transactionId;
        final long timeoutTime;

        hub.outBytesLock().lock();
        try {
            final long sizeLocation = hub.writeEventAnSkip(ENTRY_SET);
            final long startTime = System.currentTimeMillis();
            timeoutTime = System.currentTimeMillis() + hub.timeoutMs;
            transactionId = hub.send(sizeLocation, startTime);
        } finally {
            hub.outBytesLock().unlock();
        }

        // get the data back from the server
        ThreadLocalCopies copies = keyReaderWithSize.getCopies(null);
        final BytesReader<K> keyReader = keyReaderWithSize.readerForLoop(copies);
        copies = valueReaderWithSize.getCopies(copies);
        final BytesReader<V> valueReader = valueReaderWithSize.readerForLoop(copies);
        final Map<K, V> result = new HashMap<K, V>();

        for (; ; ) {

            hub.inBytesLock().lock();
            try {

                Bytes in = hub.blockingFetchReadOnly(timeoutTime, transactionId);

                final boolean hasMoreEntries = in.readBoolean();

                // number of entries in this chunk
                final long size = in.readInt();

                for (int i = 0; i < size; i++) {
                    final K k = keyReaderWithSize.readInLoop(in, keyReader);
                    final V v = valueReaderWithSize.readInLoop(in, valueReader);
                    result.put(k, v);
                }

                if (!hasMoreEntries)
                    break;

            } finally {
                hub.inBytesLock().unlock();
            }
        }

        return result.entrySet();

    }

    /**
     * @param callback each entry is passed to the callback
     */
    void entrySet(@NotNull MapEntryCallback<K, V> callback) {
        final long transactionId;
        final long timeoutTime;

        hub.outBytesLock().lock();
        try {
            final long sizeLocation = hub.writeEventAnSkip(ENTRY_SET);
            final long startTime = System.currentTimeMillis();
            timeoutTime = System.currentTimeMillis() + hub.timeoutMs;
            transactionId = hub.send(sizeLocation, startTime);
        } finally {
            hub.outBytesLock().unlock();
        }

        // get the data back from the server
        ThreadLocalCopies copies = keyReaderWithSize.getCopies(null);
        final BytesReader<K> keyReader = keyReaderWithSize.readerForLoop(copies);
        copies = valueReaderWithSize.getCopies(copies);
        final BytesReader<V> valueReader = valueReaderWithSize.readerForLoop(copies);

        for (; ; ) {

            hub.inBytesLock().lock();
            try {

                Bytes in = hub.blockingFetchReadOnly(timeoutTime, transactionId);

                final boolean hasMoreEntries = in.readBoolean();

                // number of entries in this chunk
                final long size = in.readInt();

                for (int i = 0; i < size; i++) {
                    final K k = keyReaderWithSize.readInLoop(in, keyReader);
                    final V v = valueReaderWithSize.readInLoop(in, valueReader);
                    callback.onEntry(k, v);
                }

                if (!hasMoreEntries)
                    break;

            } finally {
                hub.inBytesLock().unlock();
            }
        }


    }

    public void putAll(@NotNull Map<? extends K, ? extends V> map) {

        final long sizeLocation;

        hub.outBytesLock().lock();
        try {
            sizeLocation = putReturnsNull ? hub.writeEventAnSkip(PUT_ALL_WITHOUT_ACC) :
                    hub.writeEventAnSkip(PUT_ALL);
        } finally {
            hub.outBytesLock().unlock();
        }
        final long startTime = System.currentTimeMillis();
        final long timeoutTime = startTime + hub.timeoutMs;
        final long transactionId;
        final int numberOfEntries = map.size();


        hub.outBytesLock().lock();
        try {

            hub.outBytes().writeStopBit(numberOfEntries);
            assert hub.outBytes().limit() == hub.outBytes().capacity();
            ThreadLocalCopies copies = keyWriterWithSize.getCopies(null);
            final Object keyWriter = keyWriterWithSize.writerForLoop(copies);
            copies = valueWriterWithSize.getCopies(copies);
            final Object valueWriter = valueWriterWithSize.writerForLoop(copies);

            for (final Map.Entry<? extends K, ? extends V> e : map.entrySet()) {
                final K key = e.getKey();
                final V value = e.getValue();
                if (key == null || value == null)
                    throw new NullPointerException();

                final Class<?> keyClass = key.getClass();
                if (!kClass.isAssignableFrom(keyClass)) {
                    throw new ClassCastException("key=" + key + " is of type=" + keyClass + " " +
                            "and should" +
                            " be of type=" + kClass);
                }

                writeKeyInLoop(key, keyWriter, copies);

                final Class<?> valueClass = value.getClass();
                if (!vClass.isAssignableFrom(valueClass))
                    throw new ClassCastException("value=" + value + " is of type=" + valueClass +
                            " and " +
                            "should  be of type=" + vClass);

                writeValueInLoop(value, valueWriter, copies);

            }

            transactionId = hub.send(sizeLocation, startTime);
        } finally {
            hub.outBytesLock().unlock();
        }


        if (!putReturnsNull) {
            hub.inBytesLock().lock();
            try {
                hub.blockingFetchReadOnly(timeoutTime, transactionId);
            } finally {
                hub.inBytesLock().unlock();
            }
        }
    }

    @NotNull
    public Set<K> keySet() {
        final long transactionId;
        final long timeoutTime;

        hub.outBytesLock().lock();
        try {
            final long sizeLocation = hub.writeEventAnSkip(KEY_SET);
            final long startTime = System.currentTimeMillis();
            timeoutTime = startTime + hub.timeoutMs;
            transactionId = hub.send(sizeLocation, startTime);
        } finally {
            hub.outBytesLock().unlock();
        }

        final Set<K> result = new HashSet<>();
        final BytesReader<K> keyReader = keyReaderWithSize.readerForLoop(null);

        for (; ; ) {

            hub.inBytesLock().lock();
            try {

                final Bytes in = hub.blockingFetchReadOnly(timeoutTime, transactionId);
                final boolean hasMoreEntries = in.readBoolean();

                // number of entries in the chunk
                long size = in.readInt();

                for (int i = 0; i < size; i++) {
                    result.add(keyReaderWithSize.readInLoop(in, keyReader));
                }

                if (!hasMoreEntries)
                    break;

            } finally {
                hub.inBytesLock().unlock();
            }
        }

        return result;
    }

    private long readLong(long transactionId, long startTime) {

        assert !hub.outBytesLock().isHeldByCurrentThread();

        final long timeoutTime = startTime + hub.timeoutMs;

        // receive
        hub.inBytesLock().lock();
        try {
            return hub.blockingFetchReadOnly(timeoutTime, transactionId).readLong();
        } finally {
            hub.inBytesLock().unlock();
        }
    }

    private ThreadLocalCopies writeKey(K key) {
        return writeKey(key, null);
    }

    @SuppressWarnings("SameParameterValue")
    private ThreadLocalCopies writeKey(K key, ThreadLocalCopies copies) {
        assert hub.outBytesLock().isHeldByCurrentThread();
        assert !hub.inBytesLock().isHeldByCurrentThread();
        return keyWriterWithSize.write(hub.outBytes(), key, copies);
    }

    @SuppressWarnings("UnusedReturnValue")
    private ThreadLocalCopies writeKeyInLoop(K key, Object writer, ThreadLocalCopies copies) {

        assert hub.outBytesLock().isHeldByCurrentThread();
        assert !hub.inBytesLock().isHeldByCurrentThread();
        return keyWriterWithSize.writeInLoop(hub.outBytes(), key, writer, copies);

    }

    private ThreadLocalCopies writeValue(V value, ThreadLocalCopies copies) {
        assert hub.outBytesLock().isHeldByCurrentThread();
        assert !hub.inBytesLock().isHeldByCurrentThread();

        long start = hub.outBytes().position();

        assert hub.outBytes().position() == start;
        hub.outBytes().limit(hub.outBytes().capacity());
        return valueWriterWithSize.write(hub.outBytes(), value, copies);

    }

    @SuppressWarnings("UnusedReturnValue")
    private ThreadLocalCopies writeValueInLoop(V value, Object writer, ThreadLocalCopies copies) {
        assert hub.outBytesLock().isHeldByCurrentThread();
        assert !hub.inBytesLock().isHeldByCurrentThread();
        long start = hub.outBytes().position();


        assert hub.outBytes().position() == start;
        hub.outBytes().limit(hub.outBytes().capacity());
        return valueWriterWithSize.writeInLoop(hub.outBytes(), value, writer, copies);

    }

    private V readValue(long transactionId, long startTime, ThreadLocalCopies copies, V usingValue) {
        assert !hub.outBytesLock().isHeldByCurrentThread();

        long timeoutTime = startTime + hub.timeoutMs;

        // receive
        hub.inBytesLock().lock();
        try {

            if (usingValue != null)
                return readValue(copies, hub.blockingFetchReadOnly(timeoutTime, transactionId), usingValue);
            else

                return readValue(copies, hub.blockingFetchReadOnly(timeoutTime, transactionId));
        } finally {
            hub.inBytesLock().unlock();
        }
    }

    @Nullable
    private <O> O readObject(long transactionId, long startTime) {
        assert !hub.outBytesLock().isHeldByCurrentThread();

        long timeoutTime = startTime + hub.timeoutMs;

        // receive
        hub.inBytesLock().lock();
        try {
            return (O) hub.blockingFetchReadOnly(timeoutTime, transactionId).readObject();
        } finally {
            hub.inBytesLock().unlock();
        }
    }

    private boolean readBoolean(long transactionId, long startTime) {
        assert !hub.outBytesLock().isHeldByCurrentThread();

        long timeoutTime = startTime + hub.timeoutMs;

        // receive
        hub.inBytesLock().lock();
        try {
            return hub.blockingFetchReadOnly(timeoutTime, transactionId).readBoolean();
        } finally {
            hub.inBytesLock().unlock();
        }
    }

    private int readInt(long transactionId, long startTime) {
        assert !hub.outBytesLock().isHeldByCurrentThread();
        assert hub.inBytesLock().isHeldByCurrentThread();
        long timeoutTime = startTime + hub.timeoutMs;

        // receive
        hub.inBytesLock().lock();
        try {
            return hub.blockingFetchReadOnly(timeoutTime, transactionId).readInt();
        } finally {
            hub.inBytesLock().unlock();
        }
    }

    private V readValue(ThreadLocalCopies copies, Bytes in) {
        assert !hub.outBytesLock().isHeldByCurrentThread();
        assert hub.inBytesLock().isHeldByCurrentThread();
        return valueReaderWithSize.readNullable(in, copies, null);
    }

    private V readValue(ThreadLocalCopies copies, Bytes in, V using) {
        assert !hub.outBytesLock().isHeldByCurrentThread();
        assert hub.inBytesLock().isHeldByCurrentThread();
        return valueReaderWithSize.readNullable(in, copies, using);
    }

    @SuppressWarnings("SameParameterValue")
    private boolean fetchBoolean(@NotNull final EventId eventId, K key, V value) {
        final long startTime = System.currentTimeMillis();
        ThreadLocalCopies copies;

        long transactionId;

        hub.outBytesLock().lock();
        try {
            final long sizeLocation = hub.writeEventAnSkip(eventId);

            copies = writeKey(key);
            writeValue(value, copies);

            transactionId = hub.send(sizeLocation, startTime);
        } finally {
            hub.outBytesLock().unlock();
        }


        return readBoolean(transactionId, startTime);

    }

    @SuppressWarnings("SameParameterValue")
    private boolean fetchBoolean(@NotNull final EventId eventId, K key, V value1, V value2) {
        final long startTime = System.currentTimeMillis();
        ThreadLocalCopies copies;

        long transactionId;

        hub.outBytesLock().lock();
        try {
            final long sizeLocation = hub.writeEventAnSkip(eventId);


            copies = writeKey(key);
            copies = writeValue(value1, copies);
            writeValue(value2, copies);
            transactionId = hub.send(sizeLocation, startTime);
        } finally {
            hub.outBytesLock().unlock();
        }


        return readBoolean(transactionId, startTime);

    }

    @SuppressWarnings("SameParameterValue")
    private boolean fetchBooleanV(@NotNull final EventId eventId, V value) {
        final long startTime = System.currentTimeMillis();

        long transactionId;

        hub.outBytesLock().lock();
        try {
            final long sizeLocation = hub.writeEventAnSkip(eventId);
            writeValue(value, null);
            transactionId = hub.send(sizeLocation, startTime);
        } finally {
            hub.outBytesLock().unlock();
        }

        return readBoolean(transactionId, startTime);
    }

    @SuppressWarnings("SameParameterValue")
    private boolean fetchBooleanK(@NotNull final EventId eventId, K key) {
        final long startTime = System.currentTimeMillis();

        long transactionId;

        hub.outBytesLock().lock();
        try {
            final long sizeLocation = hub.writeEventAnSkip(eventId);
            writeKey(key, null);
            transactionId = hub.send(sizeLocation, startTime);
        } finally {
            hub.outBytesLock().unlock();
        }

        return readBoolean(transactionId, startTime);
    }

    @SuppressWarnings("SameParameterValue")
    private long fetchLong(@NotNull final EventId eventId) {
        final long startTime = System.currentTimeMillis();

        long transactionId;

        hub.outBytesLock().lock();
        try {
            final long sizeLocation = hub.writeEventAnSkip(eventId);
            transactionId = hub.send(sizeLocation, startTime);
        } finally {
            hub.outBytesLock().unlock();
        }

        return readLong(transactionId, startTime);
    }

    @SuppressWarnings("SameParameterValue")
    private boolean fetchBoolean(@NotNull final EventId eventId) {
        final long startTime = System.currentTimeMillis();

        long transactionId;

        hub.outBytesLock().lock();
        try {
            final long sizeLocation = hub.writeEventAnSkip(eventId);
            transactionId = hub.send(sizeLocation, startTime);
        } finally {
            hub.outBytesLock().unlock();
        }

        return readBoolean(transactionId, startTime);
    }

    @SuppressWarnings("SameParameterValue")
    private void fetchVoid(@NotNull final EventId eventId) {

        final long startTime = System.currentTimeMillis();

        long transactionId;

        hub.outBytesLock().lock();
        try {
            final long sizeLocation = hub.writeEventAnSkip(eventId);
            transactionId = hub.send(sizeLocation, startTime);
        } finally {
            hub.outBytesLock().unlock();
        }

        readVoid(transactionId, startTime);
    }

    private void readVoid(long transactionId, long startTime) {
        long timeoutTime = startTime + hub.timeoutMs;

        // receive
        hub.inBytesLock().lock();
        try {
            hub.blockingFetchReadOnly(timeoutTime, transactionId);
        } finally {
            hub.inBytesLock().unlock();
        }
    }

    @SuppressWarnings("SameParameterValue")
    private int fetchInt(@NotNull final EventId eventId) {
        final long startTime = System.currentTimeMillis();

        long transactionId;

        hub.outBytesLock().lock();
        try {
            final long sizeLocation = hub.writeEventAnSkip(eventId);
            transactionId = hub.send(sizeLocation, startTime);
        } finally {
            hub.outBytesLock().unlock();
        }

        return readInt(transactionId, startTime);
    }

    @Nullable
    private <R> R fetchObject(Class<R> rClass, @NotNull final EventId eventId, K key, V value) {
        final long startTime = System.currentTimeMillis();
        ThreadLocalCopies copies;

        long transactionId;

        hub.outBytesLock().lock();
        try {

            final long sizeLocation = hub.writeEventAnSkip(eventId);

            copies = writeKey(key);
            copies = writeValue(value, copies);
            transactionId = hub.send(sizeLocation, startTime);
        } finally {
            hub.outBytesLock().unlock();
        }


        if (eventReturnsNull(eventId))
            return null;

        if (rClass == vClass)
            return (R) readValue(transactionId, startTime, copies, null);
        else
            throw new UnsupportedOperationException("class of type class=" + rClass + " is not " +
                    "supported");
    }

    @Nullable
    private <R> R fetchObject(Class<R> rClass, @NotNull final EventId eventId, K key) {
        final long startTime = System.currentTimeMillis();
        ThreadLocalCopies copies;

        long transactionId;

        hub.outBytesLock().lock();
        try {

            final long sizeLocation = hub.writeEventAnSkip(eventId);

            copies = writeKey(key);
            transactionId = hub.send(sizeLocation, startTime);
        } finally {
            hub.outBytesLock().unlock();
        }

        if (eventReturnsNull(eventId))
            return null;

        if (rClass == vClass)
            return (R) readValue(transactionId, startTime, copies, null);
        else
            throw new UnsupportedOperationException("class of type class=" + rClass + " is not " +
                    "supported");
    }

    private boolean eventReturnsNull(@NotNull EventId eventId) {

        switch (eventId) {
            case PUT_ALL_WITHOUT_ACC:
            case PUT_WITHOUT_ACC:
            case REMOVE_WITHOUT_ACC:
                return true;
            default:
                return false;
        }

    }

    private void writeObject(@NotNull Object function) {

        assert hub.outBytesLock().isHeldByCurrentThread();
        assert !hub.inBytesLock().isHeldByCurrentThread();

        long start = hub.outBytes().position();
        for (; ; ) {
            try {
                hub.outBytes().writeObject(function);
                return;
            } catch (IllegalStateException e) {
                Throwable cause = e.getCause();

                if (cause instanceof IOException && cause.getMessage().contains("Not enough available space")) {
                    LOG.debug("resizing buffer, name=" + hub.name);

                    try {
                        resizeToMessageOutBuffer(start, e);
                    } catch (Exception e2) {
                        throw e;
                    }

                } else
                    throw e;
            }
        }
    }

    private void resizeToMessageOutBuffer(long start, @NotNull Exception e) throws Exception {
        assert hub.outBytesLock().isHeldByCurrentThread();
        assert !hub.inBytesLock().isHeldByCurrentThread();
        String message = e.getMessage();
        if (message.startsWith("java.io.IOException: Not enough available space for writing ")) {
            String substring = message.substring("java.io.IOException: Not enough available space for writing ".length(), message.length());
            int i = substring.indexOf(' ');
            if (i != -1) {
                int size = Integer.parseInt(substring.substring(0, i));

                long requiresExtra = size - hub.outBytes().remaining();
                hub.resizeBufferOutBuffer((int) (hub.outBytes().capacity() + requiresExtra), start);
            } else
                throw e;
        } else
            throw e;
    }

    @Nullable
    private <R> R fetchObject(@NotNull final EventId eventId, K key, @NotNull Object object) {
        final long startTime = System.currentTimeMillis();
        long transactionId;

        hub.outBytesLock().lock();
        try {
            final long sizeLocation = hub.writeEventAnSkip(eventId);
            writeKey(key);
            writeObject(object);
            transactionId = hub.send(sizeLocation, startTime);
        } finally {
            hub.outBytesLock().unlock();
        }

        if (eventReturnsNull(eventId))
            return null;

        return (R) readObject(transactionId, startTime);
    }

    static enum EventId {
        HEARTBEAT,
        STATEFUL_UPDATE,
        LONG_SIZE,
        SIZE,
        IS_EMPTY,
        CONTAINS_KEY,
        CONTAINS_VALUE,
        GET,
        PUT,
        PUT_WITHOUT_ACC,
        REMOVE,
        REMOVE_WITHOUT_ACC,
        CLEAR,
        KEY_SET,
        VALUES,
        ENTRY_SET,
        REPLACE,
        REPLACE_WITH_OLD_AND_NEW_VALUE,
        PUT_IF_ABSENT,
        REMOVE_WITH_VALUE,
        TO_STRING,
        APPLICATION_VERSION,
        PERSISTED_DATA_VERSION,
        PUT_ALL,
        PUT_ALL_WITHOUT_ACC,
        HASH_CODE,
        MAP_FOR_KEY,
        PUT_MAPPED,
        KEY_BUILDER,
        VALUE_BUILDER
    }

    class Entry implements Map.Entry<K, V> {

        final K key;
        final V value;

        /**
         * Creates new entry.
         */
        Entry(K k1, V v) {
            value = v;
            key = k1;
        }

        public final K getKey() {
            return key;
        }

        public final V getValue() {
            return value;
        }

        public final V setValue(V newValue) {
            V oldValue = value;
            StatelessChronicleMap.this.put(getKey(), newValue);
            return oldValue;
        }

        public final boolean equals(Object o) {
            if (!(o instanceof Map.Entry))
                return false;
            final Map.Entry e = (Map.Entry) o;
            final Object k1 = getKey();
            final Object k2 = e.getKey();
            if (k1 == k2 || (k1 != null && k1.equals(k2))) {
                Object v1 = getValue();
                Object v2 = e.getValue();
                if (v1 == v2 || (v1 != null && v1.equals(v2)))
                    return true;
            }
            return false;
        }

        public final int hashCode() {
            return (key == null ? 0 : key.hashCode()) ^
                    (value == null ? 0 : value.hashCode());
        }

        @NotNull
        public final String toString() {
            return getKey() + "=" + getValue();
        }
    }
}

