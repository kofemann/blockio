package org.dcache.blockio;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import org.junit.Test;
import static org.junit.Assert.*;
import org.junit.Before;

/**
 *
 * @author tigran
 */
public class PageIoTest {

    int pageSize = 128;
    ByteBuffer backend;
    PageIo pageIo;
    List<Long> flushedPages;

    @Before
    public void setUp() {
        flushedPages = new ArrayList<>();
        backend = ByteBuffer.allocate(64 * 4096);
        pageIo = new PageIo(64, pageSize,
                id -> {
                    ByteBuffer b = backend.duplicate();
                    b.position(id.intValue() * pageSize);
                    b.limit(id.intValue() * pageSize + pageSize);
                    return new Page(ByteBuffer.allocate(pageSize), new ByteBufferChannel(b.slice())) {
                        @Override
                        public synchronized void flush() throws IOException {
                            super.flush();
                            flushedPages.add(id);
                        }
                    };
                });

    }

    @Test
    public void shouldMatchWrittenData() throws IOException {

        byte[] data = new byte[8192];

        new Random(0).nextBytes(data);

        for (int i = 0; i < data.length / 64; i++) {
            pageIo.write(i * 64, ByteBuffer.wrap(data, i * 64, 64));
        }
        pageIo.flushAll();
        assertArrayEquals("invalid data", data, Arrays.copyOf(backend.array(), data.length));
    }

    @Test
    public void shouldMatchRead() throws IOException {

        byte[] data = new byte[8192];

        new Random(0).nextBytes(backend.array());

        for (int i = 0; i < data.length / 64; i++) {
            ByteBuffer b = ByteBuffer.wrap(data, i * 64, 64);
            pageIo.read(i * 64, b);
        }

        assertArrayEquals("invalid data", Arrays.copyOf(backend.array(), data.length), data);
    }

    @Test
    public void shouldAllocateOnePageOnSmallWrite() throws IOException {

        byte[] data = new byte[5];

        pageIo.write(0, ByteBuffer.wrap(data));
        assertEquals(1, pageIo.getPageCount());
    }

    @Test
    public void shouldAllocateOnePageOnSmallWriteDifferentOffset() throws IOException {

        byte[] data = new byte[5];

        pageIo.write(pageSize + 16, ByteBuffer.wrap(data));
        assertEquals(1, pageIo.getPageCount());
    }

    @Test
    public void shouldAllocateTwoPagesForNotAlignedWrite() throws IOException {

        byte[] data = new byte[5];

        pageIo.write(pageSize - 1, ByteBuffer.wrap(data));
        assertEquals(2, pageIo.getPageCount());
    }

    @Test
    public void shouldAllocateMultiplePagesOnBigWrite() throws IOException {

        byte[] data = new byte[pageSize * 4];

        pageIo.write(0, ByteBuffer.wrap(data));
        assertEquals(4, pageIo.getPageCount());
    }

    @Test
    public void shouldMatchWrittenDataAfterFlushPerWrite() throws IOException {

        byte[] data = new byte[8192];

        new Random(0).nextBytes(data);

        for (int i = 0; i < data.length / 64; i++) {
            pageIo.write(i * 64, ByteBuffer.wrap(data, i * 64, 64));
            pageIo.flushAll();
        }
        assertArrayEquals("invalid data", data, Arrays.copyOf(backend.array(), data.length));
    }

    @Test
    public void shouldFlushIfMorePagesUsed() throws Exception {

        byte[] data = new byte[pageSize];
        int extraPages = 3;

        for (int i = 0; i < pageIo.getMaxPageCount() + extraPages; i++) {
            pageIo.write(i * data.length, ByteBuffer.wrap(data));
        }
        assertEquals(extraPages, flushedPages.size());
    }
}
