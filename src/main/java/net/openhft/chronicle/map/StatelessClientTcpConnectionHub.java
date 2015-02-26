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

import com.sun.jdi.connect.spi.ClosedConnectionException;
import net.openhft.chronicle.hash.RemoteCallTimeoutException;
import net.openhft.chronicle.hash.impl.util.BuildVersion;
import net.openhft.chronicle.hash.impl.util.CloseablesManager;
import net.openhft.lang.io.ByteBufferBytes;
import net.openhft.lang.io.Bytes;
import net.openhft.lang.io.IByteBufferBytes;
import net.openhft.lang.io.NativeBytes;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

import static java.nio.ByteBuffer.allocateDirect;
import static net.openhft.chronicle.map.AbstractChannelReplicator.SIZE_OF_SIZE;
import static net.openhft.chronicle.map.AbstractChannelReplicator.SIZE_OF_TRANSACTION_ID;
import static net.openhft.chronicle.map.StatelessChronicleMap.EventId.*;

/**
 * Created by Rob Austin
 */
public class StatelessClientTcpConnectionHub {

    private static final Logger LOG = LoggerFactory.getLogger(StatelessChronicleMap.class);
    private static final byte STATELESS_CLIENT_IDENTIFIER = (byte) -127;

    protected final String name;
    protected final InetSocketAddress remoteAddress;
    protected final long timeoutMs;
    protected final int tcpBufferSize;
    private final ReentrantLock inBytesLock = new ReentrantLock(true);
    private final ReentrantLock outBytesLock = new ReentrantLock();
    private final byte[] connectionByte = new byte[1];
    private final ByteBuffer connectionOutBuffer = ByteBuffer.wrap(connectionByte);
    private final BufferResizer outBufferResizer = this::resizeBufferOutBuffer;
    @NotNull
    private final AtomicLong transactionID = new AtomicLong(0);
    private ByteBuffer outBuffer;
    private IByteBufferBytes outBytes;
    private ByteBuffer inBuffer;
    private IByteBufferBytes inBytes;
    private SocketChannel clientChannel;

    @Nullable
    protected CloseablesManager closeables;
    //  used by the enterprise version
    protected int identifier;
    long largestEntrySoFar = 0;
    // this is a transaction id and size that has been read by another thread.
    private volatile long parkedTransactionId;
    private volatile int parkedRemainingBytes;
    private volatile long parkedTransactionTimeStamp;
    private long limitOfLast = 0;

    public StatelessClientTcpConnectionHub(ChronicleMapStatelessClientBuilder config) {
        this.remoteAddress = config.remoteAddress();
        this.tcpBufferSize = config.tcpBufferSize();
        this.name = config.name();

        outBuffer = allocateDirect(128).order(ByteOrder.nativeOrder());
        inBuffer = allocateDirect(128).order(ByteOrder.nativeOrder());

        inBytes = ByteBufferBytes.wrap(inBuffer.slice());
        outBytes = ByteBufferBytes.wrap(outBuffer.slice());
        this.timeoutMs = config.timeoutMs();

        attemptConnect(remoteAddress);
        checkVersion();

    }

    private synchronized void attemptConnect(final InetSocketAddress remoteAddress) {

        // ensures that the excising connection are closed
        closeExisting();

        try {
            SocketChannel socketChannel = AbstractChannelReplicator.openSocketChannel(closeables);
            if (socketChannel.connect(remoteAddress)) {
                doHandShaking(socketChannel);
                clientChannel = socketChannel;
            }

        } catch (IOException e) {
            if (closeables != null) closeables.closeQuietly();
            clientChannel = null;
        }


    }

    ReentrantLock inBytesLock() {
        return inBytesLock;
    }

    ReentrantLock outBytesLock() {
        return outBytesLock;
    }

    protected void checkVersion() {

        String serverVersion = serverApplicationVersion();
        String clientVersion = clientVersion();

        if (!serverVersion.equals(clientVersion)) {
            LOG.warn("DIFFERENT CHRONICLE-MAP VERSIONS: The Chronicle-Map-Server and " +
                    "Stateless-Client are on different " +
                    "versions, " +
                    " we suggest that you use the same version, server=" + serverApplicationVersion() + ", " +
                    "client=" + clientVersion);
        }
    }


    private void checkTimeout(long timeoutTime) {
        if (timeoutTime < System.currentTimeMillis())
            throw new RemoteCallTimeoutException();
    }

    protected synchronized void lazyConnect(final long timeoutMs,
                                            final InetSocketAddress remoteAddress) {
        if (clientChannel != null)
            return;

        if (LOG.isDebugEnabled())
            LOG.debug("attempting to connect to " + remoteAddress + " ,name=" + name);

        SocketChannel result;

        long timeoutAt = System.currentTimeMillis() + timeoutMs;

        for (; ; ) {
            checkTimeout(timeoutAt);

            // ensures that the excising connection are closed
            closeExisting();

            try {
                result = AbstractChannelReplicator.openSocketChannel(closeables);
                if (!result.connect(remoteAddress)) {
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    continue;
                }

                result.socket().setTcpNoDelay(true);
                doHandShaking(result);
                break;
            } catch (IOException e) {
                if (closeables != null) closeables.closeQuietly();
            } catch (Exception e) {
                if (closeables != null) closeables.closeQuietly();
                throw e;
            }
        }
        clientChannel = result;
        byte[] bytes = copyBufferBytes();

        long position = outBytes.position();
        int limit = outBuffer.limit();

        outBuffer.clear();
        outBytes.clear();
        //  outBytesLock.unlock();
        // assert !outBytesLock.isHeldByCurrentThread();
        try {
            checkVersion();
        } finally {
            outBuffer.clear();
            outBuffer.put(bytes);
            outBytes.limit(limit);
            outBytes.position(position);
            //     outBytesLock.lock();
        }

    }

    private byte[] copyBufferBytes() {
        byte[] bytes = new byte[outBuffer.limit()];
        outBuffer.position(0);
        outBuffer.get(bytes);
        return bytes;
    }

    /**
     * closes the existing connections and establishes a new closeables
     */
    protected void closeExisting() {
        // ensure that any excising connection are first closed
        if (closeables != null)
            closeables.closeQuietly();

        closeables = new CloseablesManager();
    }

    /**
     * initiates a very simple level of handshaking with the remote server, we send a special ID of
     * -127 ( when the server receives this it knows its dealing with a stateless client, receive
     * back an identifier from the server
     *
     * @param clientChannel clientChannel
     * @throws java.io.IOException
     */
    protected synchronized void doHandShaking(@NotNull final SocketChannel clientChannel) throws IOException {

        connectionByte[0] = STATELESS_CLIENT_IDENTIFIER;
        this.connectionOutBuffer.clear();

        long timeoutTime = System.currentTimeMillis() + timeoutMs;

        // write a single byte
        while (connectionOutBuffer.hasRemaining()) {
            clientChannel.write(connectionOutBuffer);
            checkTimeout(timeoutTime);
        }

        this.connectionOutBuffer.clear();

        if (!clientChannel.finishConnect() || !clientChannel.socket().isBound())
            return;

        // read a single byte back
        while (this.connectionOutBuffer.position() <= 0) {
            int read = clientChannel.read(this.connectionOutBuffer);// the remote identifier
            if (read == -1)
                throw new IOException("server conncetion closed");
            checkTimeout(timeoutTime);
        }

        byte remoteIdentifier = connectionByte[0];

        if (LOG.isDebugEnabled())
            LOG.debug("Attached to a map with a remote identifier=" + remoteIdentifier + " ,name=" + name);

    }

    public synchronized void close() {

        if (closeables != null)
            closeables.closeQuietly();
        closeables = null;
        clientChannel = null;

    }

    /**
     * the transaction id are generated as unique timestamps
     *
     * @param time in milliseconds
     * @return a unique transactionId
     */
    private long nextUniqueTransaction(long time) {
        long id = time * TcpReplicator.TIMESTAMP_FACTOR;
        for (; ; ) {
            long old = transactionID.get();
            if (old >= id) id = old + 1;
            if (transactionID.compareAndSet(old, id))
                break;
        }
        return id;
    }

    @NotNull
    public String serverApplicationVersion() {
        return fetchObject(String.class, APPLICATION_VERSION);
    }

    @SuppressWarnings("WeakerAccess")
    @NotNull
    public String clientVersion() {
        return BuildVersion.version();
    }

    private Bytes resizeBufferOutBuffer(int newCapacity) {
        return resizeBufferOutBuffer(newCapacity, outBytes.position());
    }

    protected Bytes resizeBufferOutBuffer(int newCapacity, long start) {
        assert outBytesLock().isHeldByCurrentThread();
        assert !inBytesLock().isHeldByCurrentThread();

        if (LOG.isDebugEnabled())
            LOG.debug("resizing buffer to newCapacity=" + newCapacity + " ,name=" + name);

        if (newCapacity < outBuffer.capacity())
            throw new IllegalStateException("it not possible to resize the buffer smaller");

        assert newCapacity < Integer.MAX_VALUE;

        final ByteBuffer result = ByteBuffer.allocate(newCapacity).order(ByteOrder.nativeOrder());
        final long bytesPosition = outBytes.position();

        outBytes = new ByteBufferBytes(result.slice());

        outBuffer.position(0);
        outBuffer.limit((int) bytesPosition);

        int numberOfLongs = (int) bytesPosition / 8;

        // chunk in longs first
        for (int i = 0; i < numberOfLongs; i++) {
            outBytes.writeLong(outBuffer.getLong());
        }

        for (int i = numberOfLongs * 8; i < bytesPosition; i++) {
            outBytes.writeByte(outBuffer.get());
        }

        outBuffer = result;

        assert outBuffer.capacity() == outBytes.capacity();

        assert outBuffer.capacity() == newCapacity;
        assert outBuffer.capacity() == outBytes.capacity();
        assert outBytes.limit() == outBytes.capacity();
        outBytes.position(start);
        return outBytes;
    }

    private void resizeBufferInBuffer(int newCapacity, long start) {
//        assert !outBytesLock.isHeldByCurrentThread();
        assert inBytesLock().isHeldByCurrentThread();

        if (LOG.isDebugEnabled())
            LOG.debug("InBuffer resizing buffer to newCapacity=" + newCapacity + " ,name=" + name);

        if (newCapacity < inBuffer.capacity())
            throw new IllegalStateException("it not possible to resize the buffer smaller");

        assert newCapacity < Integer.MAX_VALUE;

        final ByteBuffer result = ByteBuffer.allocate(newCapacity).order(ByteOrder.nativeOrder());
        final long bytesPosition = inBytes.position();

        inBytes = new ByteBufferBytes(result.slice());

        inBuffer.position(0);
        inBuffer.limit((int) bytesPosition);

        int numberOfLongs = (int) bytesPosition / 8;

        // chunk in longs first
        for (int i = 0; i < numberOfLongs; i++) {
            inBytes.writeLong(inBuffer.getLong());
        }

        for (int i = numberOfLongs * 8; i < bytesPosition; i++) {
            inBytes.writeByte(inBuffer.get());
        }

        inBuffer = result;

        assert inBuffer.capacity() == inBytes.capacity();
        assert inBuffer.capacity() == newCapacity;
        assert inBuffer.capacity() == inBytes.capacity();
        assert inBytes.limit() == inBytes.capacity();

        inBytes.position(start);
    }

    private long writeEvent(@NotNull StatelessChronicleMap.EventId event) {
        assert outBytesLock().isHeldByCurrentThread();
        assert !inBytesLock().isHeldByCurrentThread();
        assert event != HEARTBEAT;

        if (outBytes.remaining() < 128)
            resizeBufferOutBuffer((int) outBytes.capacity() + 128, outBytes.position());

        outBytes.writeByte((byte) event.ordinal());


        return markSizeLocation();
    }

    /**
     * skips for the transactionid
     */
    protected long writeEventAnSkip(@NotNull StatelessChronicleMap.EventId event) {

        assert outBytesLock().isHeldByCurrentThread();
        assert !inBytesLock().isHeldByCurrentThread();

        final long sizeLocation = writeEvent(event);
        assert outBytes.readByte(sizeLocation - 1) == event.ordinal();

        assert outBytes.position() > 0;

        // skips for the transaction id
        outBytes.skip(SIZE_OF_TRANSACTION_ID);
        outBytes.writeByte(identifier);
        outBytes.writeInt(0);
        assert outBytes.readByte(sizeLocation - 1) == event.ordinal();
        return sizeLocation;
    }

    /**
     * sends data to the server via TCP/IP
     *
     * @param sizeLocation the position of the bit that marks the size
     * @param startTime    the current time
     * @return a unique transaction id
     */
    protected long send(long sizeLocation, final long startTime) {

        assert outBytesLock().isHeldByCurrentThread();
        assert !inBytesLock().isHeldByCurrentThread();

        long transactionId = nextUniqueTransaction(startTime);
        final long timeoutTime = startTime + this.timeoutMs;
        try {

            for (; ; ) {
                if (clientChannel == null) {
                    lazyConnect(timeoutMs, remoteAddress);
                }
                try {

                    if (LOG.isDebugEnabled())
                        LOG.debug("sending data with transactionId=" + transactionId + " ,name=" + name);

                    writeSizeAndTransactionIdAt(sizeLocation, transactionId);

                    // send out all the bytes
                    writeBytesToSocket(timeoutTime);

                    break;

                } catch (@NotNull java.nio.channels.ClosedChannelException | ClosedConnectionException e) {
                    checkTimeout(timeoutTime);
                    lazyConnect(timeoutMs, remoteAddress);
                }
            }
        } catch (IOException e) {
            close();
            throw new IORuntimeException(e);
        } catch (Exception e) {
            close();
            throw e;
        }
        return transactionId;
    }

    protected Bytes blockingFetchReadOnly(long timeoutTime, final long transactionId) {


        assert inBytesLock().isHeldByCurrentThread();
        //  assert !outBytesLock.isHeldByCurrentThread();
        try {

            return blockingFetchThrowable(timeoutTime, transactionId);
        } catch (IOException e) {
            close();
            throw new IORuntimeException(e);
        } catch (RuntimeException e) {
            close();
            throw e;
        } catch (Exception e) {
            close();
            throw new RuntimeException(e);
        } catch (AssertionError e) {
            LOG.error("name=" + name, e);
            throw e;
        }
    }

    private Bytes blockingFetchThrowable(long timeoutTime, long transactionId) throws IOException,
            InterruptedException {

//        assert !outBytesLock.isHeldByCurrentThread();
        assert inBytesLock().isHeldByCurrentThread();

        int remainingBytes = nextEntry(timeoutTime, transactionId);

        if (inBytes.capacity() < remainingBytes) {
            long pos = inBytes.position();
            long limit = inBytes.position();
            inBytes.position(limit);
            resizeBufferInBuffer(remainingBytes, pos);
        } else
            inBytes.limit(inBytes.capacity());

        // block until we have received all the bytes in this chunk
        receiveBytesFromSocket(remainingBytes, timeoutTime);

        final boolean isException = inBytes.readBoolean();

        if (isException) {
            Throwable throwable = (Throwable) inBytes.readObject();
            try {
                Field stackTrace = Throwable.class.getDeclaredField("stackTrace");
                stackTrace.setAccessible(true);
                List<StackTraceElement> stes = new ArrayList<>(Arrays.asList((StackTraceElement[]) stackTrace.get(throwable)));
                // prune the end of the stack.
                for (int i = stes.size() - 1; i > 0 && stes.get(i).getClassName().startsWith("Thread"); i--) {
                    stes.remove(i);
                }
                InetSocketAddress address = remoteAddress;
                stes.add(new StackTraceElement("~ remote", "tcp ~", address.getHostName(), address.getPort()));
                StackTraceElement[] stackTrace2 = Thread.currentThread().getStackTrace();
                //noinspection ManualArrayToCollectionCopy
                for (int i = 4; i < stackTrace2.length; i++)
                    stes.add(stackTrace2[i]);
                stackTrace.set(throwable, stes.toArray(new StackTraceElement[stes.size()]));
            } catch (Exception ignore) {
            }
            NativeBytes.UNSAFE.throwException(throwable);
        }


        return inBytes;
    }

    private int nextEntry(long timeoutTime, long transactionId) throws IOException {
//        assert !outBytesLock.isHeldByCurrentThread();
        assert inBytesLock().isHeldByCurrentThread();

        int remainingBytes;
        for (; ; ) {

            // read the next item from the socket
            if (parkedTransactionId == 0) {

                assert parkedTransactionTimeStamp == 0;
                assert parkedRemainingBytes == 0;

                receiveBytesFromSocket(SIZE_OF_SIZE + SIZE_OF_TRANSACTION_ID, timeoutTime);

                final int messageSize = inBytes.readInt();
                assert messageSize > 0 : "Invalid message size " + messageSize;
                assert messageSize < 16 << 20 : "Invalid message size " + messageSize;

                final int remainingBytes0 = messageSize - (SIZE_OF_SIZE + SIZE_OF_TRANSACTION_ID);
                final long transactionId0 = inBytes.readLong();

                // check the transaction id is reasonable
                assert transactionId0 > 1410000000000L * TcpReplicator.TIMESTAMP_FACTOR :
                        "TransactionId too small " + transactionId0 + " messageSize " + messageSize;
                assert transactionId0 < 2100000000000L * TcpReplicator.TIMESTAMP_FACTOR :
                        "TransactionId too large " + transactionId0 + " messageSize " + messageSize;

                // if the transaction id is for this thread process it
                if (transactionId0 == transactionId) {

                    // we have the correct transaction id !
                    parkedTransactionId = 0;
                    remainingBytes = remainingBytes0;
                    assert remainingBytes > 0;

                    clearParked();
                    break;

                } else {
                    // if the transaction id is not for this thread, park it and read the next one
                    parkedTransactionTimeStamp = System.currentTimeMillis();
                    parkedRemainingBytes = remainingBytes0;
                    parkedTransactionId = transactionId0;

                    pause();
                    continue;
                }
            }

            // the transaction id was read by another thread, but is for this thread, process it
            if (parkedTransactionId == transactionId) {
                remainingBytes = parkedRemainingBytes;
                clearParked();
                break;
            }


            // time out the old transaction id
            if (System.currentTimeMillis() - timeoutTime >
                    parkedTransactionTimeStamp) {

                LOG.error("name=" + name, new IllegalStateException("Skipped Message with " +
                        "transaction-id=" +
                        parkedTransactionTimeStamp +
                        ", this can occur when you have another thread which has called the " +
                        "stateless client and terminated abruptly before the message has been " +
                        "returned from the server"));

                // read the the next message
                receiveBytesFromSocket(parkedRemainingBytes, timeoutTime);
                clearParked();

            }

            pause();
        }
        return remainingBytes;
    }

    private void clearParked() {
//        assert !outBytesLock.isHeldByCurrentThread();
        assert inBytesLock().isHeldByCurrentThread();
        parkedTransactionId = 0;
        parkedRemainingBytes = 0;
        parkedTransactionTimeStamp = 0;
    }

    private void pause() {

        assert !outBytesLock().isHeldByCurrentThread();
        assert inBytesLock().isHeldByCurrentThread();

        /// don't call inBytesLock.isHeldByCurrentThread() as it not atomic
        inBytesLock().unlock();
        inBytesLock().lock();
    }

    /**
     * reads up to the number of byte in {@code requiredNumberOfBytes}
     *
     * @param requiredNumberOfBytes the number of bytes to read
     * @param timeoutTime           timeout in milliseconds
     * @return bytes read from the TCP/IP socket
     * @throws java.io.IOException socket failed to read data
     */
    @SuppressWarnings("UnusedReturnValue")
    private Bytes receiveBytesFromSocket(int requiredNumberOfBytes, long timeoutTime) throws IOException {

//        assert !outBytesLock.isHeldByCurrentThread();
        assert inBytesLock().isHeldByCurrentThread();

        inBytes.position(0);
        inBytes.limit(requiredNumberOfBytes);
        inBytes.buffer().position(0);
        inBytes.buffer().limit(requiredNumberOfBytes);

        while (inBytes.buffer().remaining() > 0) {
            assert requiredNumberOfBytes <= inBytes.capacity();

            int len = clientChannel.read(inBytes.buffer());

            if (len == -1)
                throw new IORuntimeException("Disconnection to server");

            checkTimeout(timeoutTime);
        }

        inBytes.position(0);
        inBytes.limit(requiredNumberOfBytes);
        return inBytes;
    }

    private void writeBytesToSocket(long timeoutTime) throws IOException {

        assert outBytesLock().isHeldByCurrentThread();
        assert !inBytesLock().isHeldByCurrentThread();

        // if we have other threads waiting to send and the buffer is not full, let the other threads
        // write to the buffer
        if (outBytesLock().hasQueuedThreads() &&
                outBytes.position() + largestEntrySoFar <= tcpBufferSize) {
            return;
        }

        outBuffer.limit((int) outBytes.position());
        outBuffer.position(0);

        int sizeOfThisMessage = (int) (outBuffer.limit() - limitOfLast);
        if (largestEntrySoFar < sizeOfThisMessage)
            largestEntrySoFar = sizeOfThisMessage;

        limitOfLast = outBuffer.limit();


        while (outBuffer.remaining() > 0) {

            int len = clientChannel.write(outBuffer);
            if (len == -1)
                throw new IORuntimeException("Disconnection to server");


            // if we have queued threads then we don't have to write all the bytes as the other
            // threads will write the remains bytes.
            if (outBuffer.remaining() > 0 && outBytesLock().hasQueuedThreads() &&
                    outBuffer.remaining() + largestEntrySoFar <= tcpBufferSize) {

                LOG.debug("continuing -  without all the data being written to the buffer as " +
                        "it will be written by the next thread");
                outBuffer.compact();
                outBytes.limit(outBuffer.limit());
                outBytes.position(outBuffer.position());
                return;
            }

            checkTimeout(timeoutTime);

        }

        outBuffer.clear();
        outBytes.clear();

    }

    private void writeSizeAndTransactionIdAt(long locationOfSize, final long transactionId) {

        assert outBytesLock().isHeldByCurrentThread();
        assert !inBytesLock().isHeldByCurrentThread();

        assert outBytes.readByte(locationOfSize - 1) >= LONG_SIZE.ordinal();


        final long size = outBytes.position() - locationOfSize;
        final long pos = outBytes.position();
        outBytes.position(locationOfSize);
        try {
            outBuffer.position((int) locationOfSize);

        } catch (IllegalArgumentException e) {
            LOG.error("locationOfSize=" + locationOfSize + ", limit=" + outBuffer.limit(), e);
        }
        int size0 = (int) size - SIZE_OF_SIZE;

        outBytes.writeInt(size0);
        assert transactionId != 0;
        outBytes.writeLong(transactionId);

        outBytes.position(pos);
    }

    private long markSizeLocation() {
        assert outBytesLock().isHeldByCurrentThread();
        assert !inBytesLock().isHeldByCurrentThread();

        final long position = outBytes.position();
        outBytes.writeInt(0); // skip the size
        return position;
    }

    private long send(@NotNull final StatelessChronicleMap.EventId eventId, final long startTime) {

        assert outBytesLock().isHeldByCurrentThread();
        assert !inBytesLock().isHeldByCurrentThread();

        assert eventId.ordinal() != 0;
        // send
        outBytesLock().lock();
        try {
            final long sizeLocation = writeEventAnSkip(eventId);
            assert sizeLocation == 1;
            return send(sizeLocation, startTime);
        } finally {
            outBytesLock().unlock();
        }
    }

    @SuppressWarnings("SameParameterValue")
    @Nullable
    protected <O> O fetchObject(final Class<O> tClass, @NotNull final StatelessChronicleMap.EventId eventId) {
        final long startTime = System.currentTimeMillis();
        long transactionId;

        outBytesLock().lock();
        try {
            transactionId = send(eventId, startTime);
        } finally {
            outBytesLock().unlock();
        }

        long timeoutTime = startTime + this.timeoutMs;

        // receive
        inBytesLock().lock();
        try {
            return blockingFetchReadOnly(timeoutTime, transactionId).readObject(tClass);
        } finally {
            inBytesLock().unlock();
        }
    }

    BufferResizer outBufferResizer() {
        return outBufferResizer;
    }

    public IByteBufferBytes outBytes() {
        return  outBytes;
    }
}
