package org.dcache.blockio;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ByteChannel;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import javax.annotation.concurrent.GuardedBy;

public class Page {

    /**
     * Indicates is there data which is not flushed to the disk.
     */
    private boolean isDirty;

    /**
     * The {@link ByteBuffer} where page keeps it's data.
     */
    private final ByteBuffer data;

    /**
     * number of used bytes in the page.
     */
    private int pageDataSize;

    /**
     * ByteChannel which backs this page.
     */
    private final ByteChannel channel;

    /**
     * Lock to protect access to the page.
     */
    private final ReadWriteLock rwLock;

    Page(ByteBuffer data, ByteChannel channel) {
        this.data = data;
        this.channel = channel;
        this.pageDataSize = 0;
        this.rwLock = new ReentrantReadWriteLock();
    }

    /**
     * Returns if there is data in the page which is not flushed to the back-end
     * {@code channel}.
     *
     * @return true if data is not flushed to the channel.
     */
    public synchronized boolean isDirty() {
        return isDirty;
    }

    /**
     * Write data into the page.
     *
     * @param offset the offset them the beginning of the page.
     * @param src the buffer with data to be written.
     */
    @GuardedBy("rwLock")
    public void write(int offset, ByteBuffer src) {
        if (src.hasRemaining()) {
            isDirty = true;
            data.clear().position(offset);
            data.put(src);
            pageDataSize = Math.max(pageDataSize, data.position());
        }
    }

    /**
     * Read data into {@code dest}.
     *
     * @param offset the offset from beginning of the page.
     * @param dest the buffer where to read the data.
     */
    @GuardedBy("rwLock")
    public void read(int offset, ByteBuffer dest) {
        data.clear().position(offset).limit(Math.min(pageDataSize, offset + dest.remaining()));
        dest.put(data);
    }

    public ReadWriteLock getLock() {
        return rwLock;
    }

    /**
     * Load page with data from the disk;
     *
     * @throws IOException If some other I/O error occurs
     */
    public synchronized void load() throws IOException {
        data.clear();
        channel.read(data);
        pageDataSize = data.position();
    }

    /**
     * Flush data to the disk if needed.
     *
     * @throws IOException If some other I/O error occurs
     */
    public synchronized void flush() throws IOException {
        if (isDirty) {
            data.clear();
            data.limit(pageDataSize);
            channel.write(data);
            isDirty = false;
        }
    }
}
