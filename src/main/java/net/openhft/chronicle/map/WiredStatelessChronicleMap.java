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

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.hash.function.SerializableFunction;
import net.openhft.chronicle.wire.Marshallable;
import net.openhft.chronicle.wire.Wire;
import net.openhft.lang.threadlocal.ThreadLocalCopies;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Predicate;

import static java.util.Collections.emptyList;
import static net.openhft.chronicle.map.WiredStatelessChronicleMap.EventId.*;


/**
 * @author Rob Austin.
 */
class WiredStatelessChronicleMap<K, V> implements ChronicleMap<K, V>, Cloneable {

    private static final Logger LOG = LoggerFactory.getLogger(WiredStatelessChronicleMap.class);
    private final WiredStatelessClientTcpConnectionHub hub;

    protected Class<K> kClass;
    protected Class<V> vClass;

    private boolean putReturnsNull;
    private boolean removeReturnsNull;
    private short channelID;

    WiredStatelessChronicleMap(@NotNull final WiredChronicleMapStatelessClientBuilder config,
                               @NotNull final Class kClass,
                               @NotNull final Class vClass,
                               short channelID) {
        this.channelID = channelID;
        hub = config.hub;
        this.putReturnsNull = config.putReturnsNull();
        this.removeReturnsNull = config.removeReturnsNull();
        this.kClass = kClass;
        this.vClass = vClass;
    }


    @SuppressWarnings("UnusedDeclaration")
    void identifier(int localIdentifier) {
        hub.localIdentifier = localIdentifier;
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
        return hub.serverApplicationVersion(channelID);
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

        return proxyReturnObject(PUT_IF_ABSENT.toString(), key, value, vClass);
    }

    @SuppressWarnings("NullableProblems")
    public boolean remove(Object key, Object value) {

        if (key == null)
            throw new NullPointerException();

        return value != null && proxyReturnBoolean(REMOVE_WITH_VALUE.toString(), (K) key, (V) value);

    }

    @SuppressWarnings("NullableProblems")
    public boolean replace(K key, V oldValue, V newValue) {
        if (key == null || oldValue == null || newValue == null)
            throw new NullPointerException();
        return proxyReturnBoolean(REPLACE_WITH_OLD_AND_NEW_VALUE.toString(), key, oldValue, newValue);
    }

    @SuppressWarnings("NullableProblems")
    public V replace(K key, V value) {
        if (key == null || value == null)
            throw new NullPointerException();
        return proxyReturnObject(REPLACE.toString(), key, value, vClass);
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
        return proxyReturnInt(HASH_CODE.toString());
    }

    @NotNull
    public String toString() {
        return hub.proxyReturnString(TO_STRING.toString(), channelID);
    }

    @NotNull
    public String serverPersistedDataVersion() {
        return hub.proxyReturnString(PERSISTED_DATA_VERSION.toString(), channelID);
    }

    public boolean isEmpty() {
        return proxyReturnBoolean(IS_EMPTY.toString());
    }

    public boolean containsKey(Object key) {
        return proxyReturnBooleanK(CONTAINS_KEY.toString(), (K) key);
    }

    @NotNull
    private NullPointerException keyNotNullNPE() {
        return new NullPointerException("key can not be null");
    }

    public boolean containsValue(Object value) {
        return proxyReturnBooleanV(CONTAINS_VALUE.toString(), (V) value);
    }

    public long longSize() {
        return proxyReturnLong(LONG_SIZE.toString());
    }

    @Override
    public MapKeyContext<K, V> context(K key) {
        throw new UnsupportedOperationException("Contexts are not supported by stateless clients");
    }

    public V get(Object key) {
        return proxyReturnObject(vClass, GET.toString(), (K) key);
    }

    @Nullable
    public V getUsing(K key, V usingValue) {
        throw new UnsupportedOperationException();
    }

    @NotNull
    public V acquireUsing(@NotNull K key, V usingValue) {
        throw new UnsupportedOperationException(
                "acquireUsing() is not supported for stateless clients");
    }

    @NotNull
    @Override
    public MapKeyContext<K, V> acquireContext(@NotNull K key, @NotNull V usingValue) {
        throw new UnsupportedOperationException("Contexts are not supported by stateless clients");
    }

    public V remove(Object key) {
        if (key == null)
            throw keyNotNullNPE();
        return proxyReturnObject(vClass, removeReturnsNull ? REMOVE_WITHOUT_ACC.toString() : REMOVE.toString(), (K) key);
    }

    public V put(K key, V value) {
        if (key == null || value == null)
            throw new NullPointerException();
        return proxyReturnObject(putReturnsNull ? "PUT_WITHOUT_ACC" : "PUT", key, value, vClass);
    }

    @Nullable
    public <R> R getMapped(@Nullable K key, @NotNull SerializableFunction<? super V, R> function) {
        throw new UnsupportedOperationException();
    }

    @Nullable
    @Override
    public V putMapped(@Nullable K key, @NotNull UnaryOperator<V> unaryOperator) {
        throw new UnsupportedOperationException();
    }

    public void clear() {
        proxyReturnVoid(CLEAR.toString());
    }

    @NotNull
    public Collection<V> values() {
        throw new UnsupportedOperationException();
    }

    @NotNull
    public Set<Map.Entry<K, V>> entrySet() {
        throw new UnsupportedOperationException();
    }

    /**
     * @param callback each entry is passed to the callback
     */
    void entrySet(@NotNull MapEntryCallback<K, V> callback) {
        throw new UnsupportedOperationException();

    }

    public void putAll(@NotNull Map<? extends K, ? extends V> map) {

        throw new UnsupportedOperationException();
    }

    @NotNull
    public Set<K> keySet() {
        throw new UnsupportedOperationException("todo");
    }

    private long readLong(long transactionId, long startTime) {
        assert !hub.outBytesLock().isHeldByCurrentThread();
        final long timeoutTime = startTime + hub.timeoutMs;

        // receive
        hub.inBytesLock().lock();
        try {
            Wire wire = hub.proxyReply(timeoutTime, transactionId);
            Bytes.toDebugString(wire.bytes(), 0, wire.bytes().limit());
            return wire.read(() -> "RESULT").int64();
        } finally {
            hub.inBytesLock().unlock();
        }
    }


    private void writeField(String fieldName, Object value) {

        assert hub.outBytesLock().isHeldByCurrentThread();
        assert !hub.inBytesLock().isHeldByCurrentThread();

        if (value instanceof CharSequence) {
            hub.outWire().write(() -> fieldName).text((CharSequence) value);
        } else if (value instanceof Marshallable) {
            hub.outWire().write(() -> fieldName).marshallable((Marshallable) value);
        } else {
            throw new IllegalStateException("type=" + value.getClass() + " is unsupported, it must either be of type Marshallable or CharSequence");
        }
    }


    private V readValue(long transactionId, long startTime, final V usingValue) {
        assert !hub.outBytesLock().isHeldByCurrentThread();
        long timeoutTime = startTime + hub.timeoutMs;

        hub.inBytesLock().lock();
        try {

            final Wire wireIn = hub.proxyReply(timeoutTime, transactionId);

            if (wireIn.read(() -> "RESULT_IS_NULL").bool())
                return null;

            if (StringBuilder.class.isAssignableFrom(vClass)) {
                wireIn.read(() -> "RESULT").text((StringBuilder) usingValue);
                return usingValue;
            } else if (Marshallable.class.isAssignableFrom(vClass)) {

                if (usingValue == null)
                    try {
                        V v = vClass.newInstance();
                        wireIn.read(() -> "RESULT").marshallable((Marshallable) v);
                        return v;
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }

                wireIn.read(() -> "RESULT").marshallable((Marshallable) usingValue);
                return usingValue;

            } else if (String.class.isAssignableFrom(vClass)) {
                //noinspection unchecked
                return (V) wireIn.read(() -> "RESULT").text();

            } else {
                throw new IllegalStateException("unsupported type");
            }

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
            final Wire wireIn = hub.proxyReply(timeoutTime, transactionId);
            return wireIn.read(() -> "RESULT").bool();
        } finally {
            hub.inBytesLock().unlock();
        }
    }

    private int readInt(long transactionId, long startTime) {
        assert !hub.outBytesLock().isHeldByCurrentThread();

        long timeoutTime = startTime + hub.timeoutMs;

        // receive
        hub.inBytesLock().lock();
        try {
            final Wire wireIn = hub.proxyReply(timeoutTime, transactionId);
            return wireIn.read(() -> "RESULT").int32();
        } finally {
            hub.inBytesLock().unlock();
        }
    }


    @SuppressWarnings("SameParameterValue")
    private boolean proxyReturnBoolean(@NotNull final String methodName, K key, V value) {
        final long startTime = System.currentTimeMillis();

        long transactionId;

        hub.outBytesLock().lock();
        try {
            transactionId = hub.writeHeader(startTime, channelID);
            writeField("METHOD_NAME", methodName);
            writeField("ARG_1", key);
            writeField("ARG_2", value);

            hub.writeSocket();
        } finally {
            hub.outBytesLock().unlock();
        }

        return readBoolean(transactionId, startTime);

    }


    @SuppressWarnings("SameParameterValue")
    private boolean proxyReturnBoolean(@NotNull final String methodName, K key, V value1, V value2) {
        final long startTime = System.currentTimeMillis();

        long transactionId;
        hub.outBytesLock().lock();
        try {
            transactionId = hub.writeHeader(startTime, channelID);
            writeField("METHOD_NAME", methodName);
            writeField("ARG_1", value1);
            writeField("ARG_2", value2);
            hub.writeSocket();
        } finally {
            hub.outBytesLock().unlock();
        }

        return readBoolean(transactionId, startTime);

    }

    @SuppressWarnings("SameParameterValue")
    private boolean proxyReturnBooleanV(@NotNull final String methodName, V value) {
        final long startTime = System.currentTimeMillis();

        long transactionId;
        hub.outBytesLock().lock();
        try {
            transactionId = hub.writeHeader(startTime, channelID);
            writeField("METHOD_NAME", methodName);
            writeField("ARG_1", value);
            hub.writeSocket();
        } finally {
            hub.outBytesLock().unlock();
        }

        return readBoolean(transactionId, startTime);
    }

    @SuppressWarnings("SameParameterValue")
    private boolean proxyReturnBooleanK(@NotNull final String methodName, K key) {
        final long startTime = System.currentTimeMillis();

        long transactionId;

        hub.outBytesLock().lock();
        try {
            transactionId = hub.writeHeader(startTime, channelID);
            writeField("METHOD_NAME", methodName);
            writeField("ARG_1", key);
            hub.writeSocket();
        } finally {
            hub.outBytesLock().unlock();
        }

        return readBoolean(transactionId, startTime);
    }

    @SuppressWarnings("SameParameterValue")
    private long proxyReturnLong(@NotNull final String methodName) {
        final long startTime = System.currentTimeMillis();

        long transactionId;

        hub.outBytesLock().lock();
        try {
            transactionId = hub.writeHeader(startTime, channelID);
            writeField("METHOD_NAME", methodName);
            hub.writeSocket();
        } finally {
            hub.outBytesLock().unlock();
        }

        return readLong(transactionId, startTime);
    }

    @SuppressWarnings("SameParameterValue")
    private boolean proxyReturnBoolean(@NotNull final String methodName) {
        final long startTime = System.currentTimeMillis();

        long transactionId;

        hub.outBytesLock().lock();
        try {
            transactionId = hub.writeHeader(startTime, channelID);
            writeField("METHOD_NAME", methodName);
            hub.writeSocket();
        } finally {
            hub.outBytesLock().unlock();
        }

        return readBoolean(transactionId, startTime);
    }

    @SuppressWarnings("SameParameterValue")
    private void proxyReturnVoid(@NotNull final String methodName) {

        final long startTime = System.currentTimeMillis();

        long transactionId;

        hub.outBytesLock().lock();
        try {
            transactionId = hub.writeHeader(startTime, channelID);
            writeField("METHOD_NAME", methodName);
            hub.writeSocket();
        } finally {
            hub.outBytesLock().unlock();
        }

        readVoid(transactionId, startTime);
    }


    @SuppressWarnings("SameParameterValue")
    private int proxyReturnInt(@NotNull final String methodName) {
        final long startTime = System.currentTimeMillis();

        long transactionId;

        hub.outBytesLock().lock();
        try {
            transactionId = hub.writeHeader(startTime, channelID);
            writeField("METHOD_NAME", methodName);
            hub.writeSocket();
        } finally {
            hub.outBytesLock().unlock();
        }

        return readInt(transactionId, startTime);
    }

    @Nullable
    private <R> R proxyReturnObject(@NotNull final String methodName, K key, V value, Class<V> resultType) {

        final long startTime = System.currentTimeMillis();
        long transactionId;

        hub.outBytesLock().lock();
        try {
            assert hub.outBytesLock().isHeldByCurrentThread();
            assert !hub.inBytesLock().isHeldByCurrentThread();

            transactionId = hub.writeHeader(startTime, channelID);
            writeField("METHOD_NAME", methodName);
            writeField("ARG_1", key);
            writeField("ARG_2", value);
            hub.writeSocket();

        } finally {
            hub.outBytesLock().unlock();
        }

        if (eventReturnsNull(methodName))
            return null;

        if (resultType == vClass)
            return (R) readValue(transactionId, startTime, null);

        else
            throw new UnsupportedOperationException("class of type class=" + resultType + " is not " +
                    "supported");
    }

    @Nullable
    private <R> R proxyReturnObject(Class<R> rClass, @NotNull final String methodName, Object key) {
        final long startTime = System.currentTimeMillis();
        ThreadLocalCopies copies;

        long transactionId;

        hub.outBytesLock().lock();
        try {
            transactionId = hub.writeHeader(startTime, channelID);
            writeField("METHOD_NAME", methodName);
            writeField("ARG_1", key);
            hub.writeSocket();
        } finally {
            hub.outBytesLock().unlock();
        }

        if (eventReturnsNull(methodName))
            return null;

        if (rClass == vClass)
            return (R) readValue(transactionId, startTime, null);
        else
            throw new UnsupportedOperationException("class of type class=" + rClass + " is not " +
                    "supported");
    }

    private boolean eventReturnsNull(@NotNull String methodName) {

        switch (methodName) {
            case "PUT_ALL_WITHOUT_ACC":
            case "PUT_WITHOUT_ACC":
            case "REMOVE_WITHOUT_ACC":
                return true;
            default:
                return false;
        }

    }

    private void readVoid(long transactionId, long startTime) {
        long timeoutTime = startTime + hub.timeoutMs;

        // receive
        hub.inBytesLock().lock();
        try {
            hub.proxyReply(timeoutTime, transactionId);
        } finally {
            hub.inBytesLock().unlock();
        }
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
            WiredStatelessChronicleMap.this.put(getKey(), newValue);
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

