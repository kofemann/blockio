package org.dcache.blockio;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.junit.Test;

import static org.junit.Assert.*;

/** */
public class PageTest {

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
  public void shouldRemoveDirtyFlagAfterFlush() throws IOException {

    Page p = new Page(ByteBuffer.allocate(64), new ByteBufferChannel(8192));
    p.load();

    ByteBuffer b = ByteBuffer.allocate(64);
    b.putLong(5);
    b.flip();

    p.write(0, b);
    p.flush();
    assertFalse("Page dirty after flush", p.isDirty());
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
    assertEquals(
        "Extected read size must be last write offset + nbytes", 5 + Long.BYTES, b.position());
  }

  @Test
  public void shouldKeepPageSizeAfterWrite() throws IOException {

    Page p = new Page(ByteBuffer.allocate(64), new ByteBufferChannel(8192));
    p.load();

    ByteBuffer b = ByteBuffer.allocate(64);
    b.putLong(5);
    b.flip();

    p.write(Long.BYTES, b);

    b.clear();
    b.putLong(1);
    b.flip();
    p.write(0, b);

    b.clear();
    p.read(0, b);
    b.flip();
    assertEquals("Invalid number of bytes", 2 * Long.BYTES, b.remaining());
    assertEquals("Invalid data", 1, b.getLong());
    assertEquals("Invalid data", 5, b.getLong());
  }

  @Test
  public void shouldNotMarkPageDirtyOnZeroBytesWrite() throws IOException {

    Page p = new Page(ByteBuffer.allocate(64), new ByteBufferChannel(8192));
    p.load();

    ByteBuffer b = ByteBuffer.allocate(64);
    b.putLong(5);
    b.flip();

    p.write(0, b);
    p.flush();

    b.clear().flip();
    p.write(0, b);
    assertFalse("Page dirty after flush", p.isDirty());
  }
}
