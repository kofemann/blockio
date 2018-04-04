package org.dcache.blockio;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
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

    @Before
    public void setUp() {
        backend = ByteBuffer.allocate(64 * 4096);
        pageIo = new PageIo(64, pageSize,
                id -> {
                    ByteBuffer b = backend.duplicate();
                    b.position(id.intValue() * pageSize);
                    b.limit(id.intValue() * pageSize + pageSize);
                    return new Page(ByteBuffer.allocate(pageSize), new ByteBufferChannel(b.slice()));
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

}
