package org.dcache.blockio;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.locks.StampedLock;

import static com.google.common.base.Preconditions.checkArgument;

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
     * {@link FileChannel} used to store/retreive data.
     */
    private final FileChannel channel;

    /**
     * Lock to protect page eviction.
     */
    private final StampedLock lock;

    /**
     * Maximal number of cached pages.
     */
    private final int count;

    /**
     * Create new PageIo backed by given {@link FileChannel}.
     *
     * @param count the total number of cached pages.
     * @param pageSize single page size
     * @param channel {@link FileChannel} used to store the data.
     */
    public PageIo(int count, int pageSize, FileChannel channel) {

        checkArgument(count > 0, "Cache size must be at lease one (1).");
        checkArgument(Long.bitCount(pageSize) == 1, "Page size must be power of two (2)");

        this.pageSize = pageSize;
        this.channel = channel;
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
                     * We have exclusive lock and cat check that by re-trying the
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

                p = new Page(ByteBuffer.allocate(pageSize), pageId * pageSize, channel);
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

        long o = offset;
        while (data.hasRemaining()) {

            final long pageId = offset / pageSize;

            final Page p = getPage(pageId);
            try {
                final long pageOffset = p.getPageOffset();
                final int offsetInPage = (int) (o - pageOffset);

                ByteBuffer b = data.slice();
                int ioSize = Math.min(b.remaining(), pageSize - offsetInPage);
                b.limit(ioSize);
                p.write(offsetInPage, b);
                o += b.position();
                data.position(data.position() + b.position());
            } finally {
                p.decRefCount();
            }
        }
    }

    public int read(long offset, ByteBuffer data) throws IOException {
        int n = 0;
        long o = offset;
        while (data.hasRemaining()) {
            final long pageId = offset / pageSize;

            final Page p = getPage(pageId);
            try {
                final long pageOffset = p.getPageOffset();
                final int offsetInPage = (int) (o - pageOffset);

                ByteBuffer b = data.slice();
                int ioSize = Math.min(b.remaining(), pageSize - offsetInPage);
                b.limit(ioSize);
                p.read(offsetInPage, b);
                if (b.position() == 0) {
                    break;
                }
                o += b.position();
                n += b.position();
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

    public static void main(String args[]) throws IOException {

        FileChannel in = FileChannel.open(new File("/home/tigran/Downloads/Lightroom_6_LS11.exe").toPath());
        FileChannel out = FileChannel.open(new File("/tmp/data").toPath(),
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING,
                StandardOpenOption.WRITE,
                StandardOpenOption.READ);

        PageIo inPageCache = new PageIo(8192, 64 * 1024, in);
        PageIo outPageCache = new PageIo(64, 8192, out);

        ByteBuffer b = ByteBuffer.allocate(4096);

        long offset = 0;
        while (true) {
            b.clear();
            int n = inPageCache.read(offset, b);
            if (n == 0) {
                break;
            }
            b.flip();
            outPageCache.write(offset, b);
            offset += n;
            //  System.out.print(new String(b.array(), 0, b.remaining(), StandardCharsets.UTF_8));
        }
        System.out.println();
        inPageCache.flushAll();
        outPageCache.flushAll();
    }
}
