package net.openhft.chronicle.map;

import net.openhft.lang.io.Bytes;

/**
 * Created by Rob Austin
 */
interface MapIOBuffer {

    void ensureBufferSize(long l);

    Bytes in();
}
