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

    private final Map<Long, Page> cache;

    private final int pageSize;
    private final FileChannel channel;

    private IOException ioError;

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

        cache = new HashMap<>();
        lock = new StampedLock();
    }

    private Page getPage(long pageId) throws IOException {

        Page p = null;
        long sl = lock.readLock();
        try {
            while(true) {
                p = cache.get(pageId);
                if (p != null) {
                    break;
                }

                // try to upgrade to a write lock
                long wl = lock.tryConvertToWriteLock(sl);
                if (wl == 0) {
                    // failed to covert to write lock.
                    lock.unlockRead(sl);
                    sl = lock.writeLock();

                    // retry the loop - either entry will be in the cache, inserted
                    // by an other threas, or we get have exclusive lock to insert it.
                    continue;
                }

                // lucky thread with write access
                sl = wl;

                if (cache.size() > count - 1) {
                    // we need to free a page
                    // FIXME: we simply kill the first key without wating for release
                    Optional<Long> oid = cache.keySet()
                            .stream()
                            .findAny();

                    if (oid.isPresent()) {
                        Page evicted = cache.remove(oid.get());

                        // FIXME: wait for ref-count == 0
                        evicted.flush();
                    }
                }

                p = new Page(ByteBuffer.allocate(pageSize), pageId * pageSize, channel);
                cache.put(pageId, p);
                p.load();
            }
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
            final long pageOffset = p.getPageOffset();
            final int offsetInPage = (int) (o - pageOffset);

            ByteBuffer b = data.slice();
            int ioSize = Math.min(b.remaining(), pageSize - offsetInPage);
            b.limit(ioSize);
            p.write(offsetInPage, b);
            o += b.position();
            data.position(data.position() + b.position());
        }
    }

    public int read(long offset, ByteBuffer data) throws IOException {
        int n = 0;
        long o = offset;
        while (data.hasRemaining()) {
            final long pageId = offset / pageSize;

            final Page p = getPage(pageId);
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
        }
        return n;
    }

    public void flushAll() throws IOException {
        long stamp = lock.writeLock();
        try {
            cache.forEach((k, v) -> {
                try {
                    v.flush();
                } catch (IOException e) {
                    ioError = e;
                }
            });

            if (ioError != null) {
                throw ioError;
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
