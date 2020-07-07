package org.dcache.blockio;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class PageIo {

  /** {@link Map} to keep mapping between page number and page. */
  private final PageCache pages;

  /** Page size in bytes. Must be power of two. */
  private final int pageSize;

  /** Maximal number of cached pages. */
  private final int count;

  /**
   * Create new PageIo backed by given {@link FileChannel}.
   *
   * @param size the total number of cached pages.
   * @param pageSize single page size
   * @param supplier supplier to produce/allocate pages.
   */
  public PageIo(int size, int pageSize, Function<Long, Page> supplier) {

    requireNonNull(supplier, "Page supplier can't be null");
    checkArgument(size > 0, "Cache size must be at lease one (1).");
    checkArgument(Long.bitCount(pageSize) == 1, "Page size must be power of two (2)");

    this.pageSize = pageSize;
    this.count = size;

    pages = new PageCache(size, supplier);
  }

  public void write(long offset, ByteBuffer data) throws IOException {

    long pos = offset;

    while (data.hasRemaining()) {

      final long pageId = pos / pageSize;
      final int offsetInPage = (int) (pos % pageSize);

      final Page p = pages.get(pageId);
      Lock l = p.getLock().writeLock();
      l.lock();
      try {
        ByteBuffer b = data.slice();
        int ioSize = Math.min(b.remaining(), pageSize - offsetInPage);
        b.limit(ioSize);
        p.write(offsetInPage, b);
        pos += b.position();
        data.position(data.position() + b.position());
      } finally {
        l.unlock();
      }
    }
  }

  public int read(long offset, ByteBuffer data) throws IOException {
    int n = 0;
    long pos = offset;
    while (data.hasRemaining()) {
      final long pageId = pos / pageSize;
      final int offsetInPage = (int) (pos % pageSize);

      final Page p = pages.get(pageId);
      Lock l = p.getLock().readLock();
      l.lock();
      try {

        ByteBuffer b = data.slice();
        int ioSize = Math.min(b.remaining(), pageSize - offsetInPage);
        b.limit(ioSize);
        p.read(offsetInPage, b);
        if (b.position() == 0) {
          break;
        }
        n += b.position();
        pos += b.position();
        data.position(data.position() + b.position());
      } finally {
        l.unlock();
      }
    }
    return n;
  }

  public void flushAll() throws IOException {
    pages.flushAll();
  }

  /**
   * Returns page size in bytes.
   *
   * @return page size in bytes.
   */
  public int getPageSize() {
    return pageSize;
  }

  /**
   * Returns maximal number of pages in the page cache.
   *
   * @return maximal number of pages in the page cache.
   */
  public int getMaxPageCount() {
    return count;
  }

  /**
   * Returns number of pages in the page cache.
   *
   * @return number of pages in the page cache.
   */
  public long getPageCount() {
    return pages.size();
  }
}
