package org.dcache.blockio;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ByteChannel;
import java.nio.channels.FileChannel;

/** A {@link ByteChannel} backed with a fraction of a file. */
class PageFileChannel implements ByteChannel {

  private final FileChannel delegate;
  private final long offset;

  public PageFileChannel(FileChannel delegate, long offset) {
    this.delegate = delegate;
    this.offset = offset;
  }

  @Override
  public int read(ByteBuffer dst) throws IOException {
    return delegate.read(dst, offset);
  }

  @Override
  public boolean isOpen() {
    return delegate.isOpen();
  }

  @Override
  public void close() throws IOException {
    delegate.close();
  }

  @Override
  public int write(ByteBuffer src) throws IOException {
    return delegate.write(src, offset);
  }
}
