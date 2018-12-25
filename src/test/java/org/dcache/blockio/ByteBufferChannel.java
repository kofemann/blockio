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
        data.limit(data.position());
    }

    public ByteBufferChannel(ByteBuffer buf) {
        data = buf;
    }

    @Override
    public int read(ByteBuffer dst) throws IOException {
        ByteBuffer b = data.slice();
        b.limit(Math.min(b.remaining(), dst.remaining()));
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
