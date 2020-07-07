package org.dcache.blockio;

import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.locks.Lock;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** */
public class PageCache {

  private static final Logger LOGGER = LoggerFactory.getLogger(PageCache.class);

  private final LoadingCache<Long, Page> pages;

  public PageCache(int size, Function<Long, Page> supplier) {

    LOGGER.debug("Initializing new PageCache with size {}", size);
    pages =
        CacheBuilder.newBuilder()
            .maximumSize(size)
            .removalListener(new DirtyPageFlusher())
            .build(new PageLoader(supplier));
  }

  public static class PageLoader extends CacheLoader<Long, Page> {

    private final Function<Long, Page> supplier;

    private PageLoader(Function<Long, Page> supplier) {
      this.supplier = supplier;
    }

    @Override
    public Page load(Long k) throws Exception {
      LOGGER.debug("Loading page {}", k);
      Page p = supplier.apply(k);
      p.load();
      return p;
    }
  }

  public static class DirtyPageFlusher implements RemovalListener<Long, Page> {

    @Override
    public void onRemoval(RemovalNotification<Long, Page> rn) {
      try {
        LOGGER.debug("Removing page {} due to {}", rn.getKey(), rn.getCause());
        Page p = rn.getValue();
        Lock l = p.getLock().writeLock();
        l.lock();
        try {
          p.flush();
        } finally {
          l.unlock();
        }
      } catch (IOException e) {
        LOGGER.error("Falied to fluch page {} : {}", rn.getKey(), e.getMessage());
      }
    }
  }

  public Page get(long id) throws IOException {
    try {
      return pages.get(id);
    } catch (ExecutionException e) {
      Throwable t = e.getCause();

      LOGGER.error("Failed do load page {} : {}", id, t.getMessage());
      Throwables.throwIfUnchecked(t);
      Throwables.throwIfInstanceOf(t, IOException.class);
      throw new IOException(t);
    }
  }

  public void flushAll() {
    pages.invalidateAll();
    pages.cleanUp();
  }

  public long size() {
    return pages.size();
  }
}
