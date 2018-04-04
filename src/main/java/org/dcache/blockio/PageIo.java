package org.dcache.blockio;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.locks.StampedLock;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class PageIo {

    /**
     * {@link Map} to keep mapping between page number and page.
     */
    private final Map<Long, Page> pages;

    /**
     * Page size in bytes. Must be power of two.
     */
    private final int pageSize;

    /**
     * Lock to protect page eviction.
     */
    private final StampedLock lock;

    /**
     * Maximal number of cached pages.
     */
    private final int count;

    /**
     * Page supplier. Creates a new pages for a given pageId.
     */
    private final Function<Long, Page> pageSupplier;

    /**
     * Create new PageIo backed by given {@link FileChannel}.
     *
     * @param count the total number of cached pages.
     * @param pageSize single page size
     * @param supplier supplier to produce/allocate pages.
     */
    public PageIo(int count, int pageSize, Function<Long, Page> supplier) {

        requireNonNull(supplier, "Page supplier can't be null");
        checkArgument(count > 0, "Cache size must be at lease one (1).");
        checkArgument(Long.bitCount(pageSize) == 1, "Page size must be power of two (2)");

        this.pageSize = pageSize;
        this.pageSupplier = supplier;
        this.count = count;

        pages = new HashMap<>();
        lock = new StampedLock();
    }

    private Page getPage(long pageId) throws IOException {

        Page p = null;
        long sl = lock.readLock();
        try {
            while (true) {
                p = pages.get(pageId);
                if (p != null) {
                    break;
                }

                // try to upgrade to a write lock
                long wl = lock.tryConvertToWriteLock(sl);
                if (wl == 0) {
                    // failed to covert to write lock.
                    lock.unlockRead(sl);
                    sl = lock.writeLock();

                    /*
                     * as we have released lock an other thread for the same pageId
                     * might have won the race and insered in to the pages map.
                     *
                     * We have exclusive lock and can check that by re-trying the
                     * request.
                     */
                    continue;
                }

                // lucky thread with write access
                sl = wl;

                if (pages.size() > count - 1) {
                    // Take the LRU page to remove
                    Optional<Map.Entry<Long, Page>> oid = pages.entrySet()
                            .stream()
                            .max((e1, e2) -> Long.compare(e2.getValue().getLastAccessTime(), e1.getValue().getLastAccessTime()));

                    if (oid.isPresent()) {
                        Page evicted = pages.remove(oid.get().getKey());

                        boolean interrupted = false;
                        synchronized (evicted) {
                            while (evicted.getRefCount() > 0) {
                                try {
                                    evicted.wait();
                                } catch (InterruptedException e) {
                                    interrupted = true;
                                }
                            }
                        }

                        if (interrupted) {
                            Thread.currentThread().interrupt();
                        }

                        evicted.flush();
                    }
                }

                p = pageSupplier.apply(pageId);
                pages.put(pageId, p);
                p.load();
            }
            p.incRefCount();
            return p;
        } finally {
            lock.unlock(sl);
        }
    }

    public void write(long offset, ByteBuffer data) throws IOException {

        long pos = offset;

        while (data.hasRemaining()) {

            final long pageId = pos / pageSize;
            final int offsetInPage = (int) (pos % pageSize);

            final Page p = getPage(pageId);
            try {
                ByteBuffer b = data.slice();
                int ioSize = Math.min(b.remaining(), pageSize - offsetInPage);
                b.limit(ioSize);
                p.write(offsetInPage, b);
                pos += b.position();
                data.position(data.position() + b.position());
            } finally {
                p.decRefCount();
            }
        }
    }

    public int read(long offset, ByteBuffer data) throws IOException {
        int n = 0;
        long pos = offset;
        while (data.hasRemaining()) {
            final long pageId = pos / pageSize;
            final int offsetInPage = (int)(pos % pageSize);

            final Page p = getPage(pageId);
            try {

                ByteBuffer b = data.slice();
                int ioSize = Math.min(b.remaining(), pageSize - offsetInPage);
                b.limit(ioSize);
                p.read(offsetInPage, b);
                if (b.position() == 0) {
                    break;
                }
                n += b.position();
                pos += b.position();
                data.position(data.position() + b.position());
            } finally {
                p.decRefCount();
            }
        }
        return n;
    }

    public void flushAll() throws IOException {

        IOException ioError[] = new IOException[1];

        long stamp = lock.writeLock();
        try {
            pages.forEach((k, v) -> {
                try {
                    v.flush();
                } catch (IOException e) {
                    ioError[0] = e;
                }
            });

            if (ioError[0] != null) {
                throw ioError[0];
            }

        } finally {
            lock.unlock(stamp);
        }
    }

    /**
     * Returns page size in bytes.
     *
     * @return page size in bytes.
     */
    public int getPageSize() {
        return pageSize;
    }

    /**
     * Returns maximal number of pages in the page cache.
     *
     * @return maximal number of pages in the page cache.
     */
    public int getMaxPageCount() {
        return count;
    }

    /**
     * Returns number of pages in the page cache.
     *
     * @return number of pages in the page cache.
     */
    public int getPageCount() {
        long sl = lock.readLock();
        try {
            return pages.size();
        } finally {
            lock.unlock(sl);
        }
    }

}
