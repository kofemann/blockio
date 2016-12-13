package org.dcache.blockio;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

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
     * Offset in the file where this page starts.
     */
    private final long offset;

    /**
     * number of used bytes in the page.
     */
    private int pageDataSize;

    /**
     * FileChannle which backs this page.
     */
    private final FileChannel channel;

    /**
     * Number of active users of the page
     */
    private int refCounf;

    /**
     * Page's last access time;
     */
    private long lastAccessTime;

    Page(ByteBuffer data, long offset, FileChannel channel) {
        this.data = data;
        this.channel = channel;
        this.offset = offset;
        this.pageDataSize = 0;
        this.lastAccessTime = System.currentTimeMillis();
    }

    /**
     * Returns if there is data in the page which is not flushed to the
     * back-end {@code channel}.
     * @return true if data is not flushed to the channel.
     */
    public boolean isDirty() {
        return isDirty;
    }

    /**
     * Write data into the page.
     *
     * @param offset the offset them the beginning of the page.
     * @param src the buffer with data to be written.
     */
    public synchronized void write(int offset, ByteBuffer src) {
        isDirty = true;
        data.clear().position(offset);
        data.put(src);
        pageDataSize = data.position();
    }

    /**
     * Read data into {@code dest}.
     *
     * @param offset the offset from beginning of the page.
     * @param dest the buffer where to read the data.
     */
    public synchronized void read(int offset, ByteBuffer dest) {
        data.clear().position(offset).limit(Math.min(pageDataSize, offset + dest.remaining()));
        dest.put(data);
    }

    /**
     * File back-end file offset which matches page beginning.
     * @return offset in the file where pages starts.
     */
    public long getPageOffset() {
        return offset;
    }

    /**
     * Increase number of reference to this page by one.
     */
    public synchronized void incRefCount() {
        lastAccessTime = System.currentTimeMillis();
        refCounf++;
        notifyAll();
    }

    /**
     * Decrease number of reference to this page by one.
     */
    public synchronized void decRefCount() {
        refCounf--;
        notifyAll();
    }

    /**
     * Get number of references to this page.
     * @return number of references to the page.
     */
    public synchronized int getRefCount() {
        return refCounf;
    }

    /**
     * Returns last access time to this page in milliseconds.
     * @return page's last access time in milliseconds.
     */
    public synchronized long getLastAccessTime() {
        return lastAccessTime;
    }

    /**
     * Load page with data from the disk;
     * @throws IOException If some other I/O error occurs
     */
    public synchronized void load() throws IOException {
        data.clear();
        channel.read(data, offset);
        pageDataSize = data.position();
    }

    /**
     * Flush data to the disk if needed.
     * @throws IOException If some other I/O error occurs
     */
    public synchronized void flush() throws IOException {
        if (isDirty) {
            data.clear();
            data.limit(pageDataSize);
            channel.write(data, offset);
            isDirty = false;
        }
    }
}
