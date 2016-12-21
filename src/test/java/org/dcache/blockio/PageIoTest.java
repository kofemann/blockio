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
        pageIo = new PageIo(64, 128,
                id -> {
                    ByteBuffer b = backend.duplicate();
                    b.position(id.intValue() * 128);
                    b.limit(id.intValue() * 128 + 128);
                    return new Page(ByteBuffer.allocate(128), new ByteBufferChannel(b.slice()));
                });

    }

    @Test
    public void shouldMatchWrittenData() throws IOException {

        byte[] data = new byte[8192];

        new Random().nextBytes(data);

        for (int i = 0; i < data.length / 64; i++) {
            pageIo.write(i * 64, ByteBuffer.wrap(data, i * 64, 64));
        }
        pageIo.flushAll();
        assertArrayEquals("invalid data", data, Arrays.copyOf(backend.array(), data.length));
    }

    @Test
    public void shouldMatchWrittenDataAfterFlushPerWrite() throws IOException {

        byte[] data = new byte[8192];

        new Random().nextBytes(data);

        for (int i = 0; i < data.length / 64; i++) {
            pageIo.write(i * 64, ByteBuffer.wrap(data, i * 64, 64));
            pageIo.flushAll();
        }
        assertArrayEquals("invalid data", data, Arrays.copyOf(backend.array(), data.length));
    }

}
