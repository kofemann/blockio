package org.dcache.blockio;

import com.sun.org.apache.bcel.internal.generic.LoadClass;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ByteChannel;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 */
public class PageTest {

    private final class ByteBufferChannel implements ByteChannel {

        private final ByteBuffer data;

        public ByteBufferChannel(int size) {
            data = ByteBuffer.allocate(size);
        }

        @Override
        public int read(ByteBuffer dst) throws IOException {
            if (!data.hasRemaining()) {
                return -1;
            }
            ByteBuffer b = data.duplicate();
            b.flip();
            b.limit(Math.min(b.position(), dst.remaining()));
            dst.put(b);
            return b.position();
        }

        @Override
        public boolean isOpen() {
            return true;
        }

        @Override
        public void close() throws IOException {
            // NOP
        }

        @Override
        public int write(ByteBuffer src) throws IOException {
            data.clear();
            data.put(src);
            return data.position();
        }

    }

    @Test
    public void shouldNotMakePageDirtyOnRead() throws IOException {

        Page p = new Page(ByteBuffer.allocate(64), new ByteBufferChannel(8192));
        p.load();

        assertFalse("Not updated page can't be dirty", p.isDirty());

        p.read(0, ByteBuffer.allocate(8));
        assertFalse("Not updated page can't be dirty", p.isDirty());
    }

    @Test
    public void shouldMarkPageDirtyOnWrite() throws IOException {

        Page p = new Page(ByteBuffer.allocate(64), new ByteBufferChannel(8192));
        p.load();

        ByteBuffer b = ByteBuffer.allocate(64);
        b.putLong(5);

        p.write(0, b);
        assertTrue("Updated must make page dirty", p.isDirty());
    }

    @Test
    public void shouldSetPageSizeToOffsetLenOnWrite() throws IOException {

        Page p = new Page(ByteBuffer.allocate(64), new ByteBufferChannel(8192));
        p.load();

        ByteBuffer b = ByteBuffer.allocate(64);
        b.putLong(5);

        b.flip();
        p.write(5, b);

        b.clear();
        p.read(0, b);
        assertEquals("Extected read size must be last write offset + nbytes",
                5 + Long.BYTES , b.position());
    }
}
