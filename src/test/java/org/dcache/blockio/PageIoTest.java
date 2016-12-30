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

    ByteBuffer backend;
    PageIo pageIo;

    @Before
    public void setUp() {
        backend = ByteBuffer.allocate(64 * 4096);
        int pageSize = 128;
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
