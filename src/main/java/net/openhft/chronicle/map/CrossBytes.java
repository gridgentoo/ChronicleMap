package net.openhft.chronicle.map;

import net.openhft.lang.io.MultiStoreBytes;

/**
 * Created by Rob Austin
 */
public interface CrossBytes {
    void write(MultiStoreBytes entry, long valueOffset, long valueSize);

    void writeBoolean(boolean b);
}
