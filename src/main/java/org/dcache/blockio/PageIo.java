package org.dcache.blockio;

import com.google.common.base.Throwables;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalNotification;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.ExecutionException;

import static com.google.common.base.Preconditions.checkArgument;

public class PageIo {

    private final Cache<Long, Page> cache;

    private final int pageSize;
    private final FileChannel channel;

    private IOException ioError;

    /**
     * Create new PageIo backed by given {@link FileChannel}.
     * @param count the total number of cached pages.
     * @param pageSize single page size
     * @param channel {@link FileChannel} used to store the data.
     */
    public PageIo(int count, int pageSize, FileChannel channel) {

        checkArgument(count > 0, "Cache size must be at lease one (1).");
        checkArgument(Long.bitCount(pageSize) == 1, "Page size must be power of two (2)");

        this.pageSize = pageSize;
        this.channel = channel;

        cache = CacheBuilder.newBuilder()
                .maximumSize(count)
                .removalListener((RemovalNotification<Long, Page> rn) -> {
                    try {
                        Page p = rn.getValue();
                        p.flush();
                    } catch (IOException e) {
                        ioError = e;
                    }
                })
                .build();
    }

    private Page getPage(long pageId) throws IOException {

        try {
            Page p = cache.get(pageId, () -> {
                Page pp = new Page(ByteBuffer.allocate(pageSize), pageId * pageSize, channel);
                pp.load();
                return pp;
            });
             // getting Page from cache may trigger page load/flush and IO error.
            if (ioError != null) {
                throw ioError;
            }
            return p;
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            Throwables.throwIfInstanceOf(cause, IOException.class);
            throw new IOException(cause);
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
        cache.invalidateAll();
        if (ioError != null) {
            throw ioError;
        }
    }

    public static void main(String args[]) throws IOException {

        FileChannel in = FileChannel.open(new File("/home/tigran/Downloads/Lightroom_6_LS11.exe").toPath());
        FileChannel out = FileChannel.open(new File("/tmp/data").toPath(),
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING,
                StandardOpenOption.WRITE,
                StandardOpenOption.READ);

        PageIo inPageCache = new PageIo(64 * 1024, 512, in);
        PageIo outPageCache = new PageIo(64 * 1024, 8192, out);

        ByteBuffer b = ByteBuffer.allocate(64);

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
            System.out.print(new String(b.array(), 0, b.remaining(), StandardCharsets.UTF_8));
        }
        System.out.println();

        outPageCache.flushAll();
    }
}
