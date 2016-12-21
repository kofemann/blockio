package org.dcache.blockio;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ByteChannel;

/**
 *
 */
class ByteBufferChannel implements ByteChannel {

    private final ByteBuffer data;

    public ByteBufferChannel(int size) {
        this(ByteBuffer.allocate(size));
    }

    public ByteBufferChannel(ByteBuffer buf) {
        data = buf;
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
