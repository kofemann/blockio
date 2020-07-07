package org.dcache.blockio;

import com.fasterxml.jackson.databind.util.ByteBufferBackedInputStream;
import com.fasterxml.jackson.databind.util.ByteBufferBackedOutputStream;
import com.google.common.io.ByteStreams;
import io.minio.ErrorCode;
import io.minio.MinioClient;
import io.minio.errors.ErrorResponseException;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.ByteChannel;

/** */
public class S3Block implements ByteChannel {

  private static final String MIME_TYPE = "binary/octet-stream";

  private final MinioClient s3;
  private final String bucket;
  private final String objName;

  public S3Block(MinioClient s3, String bucket, String objName) {
    this.s3 = s3;
    this.bucket = bucket;
    this.objName = objName;
  }

  @Override
  public int read(ByteBuffer dst) throws IOException {

    try {
      InputStream stream = s3.getObject(bucket, objName);

      ByteBufferBackedOutputStream out = new ByteBufferBackedOutputStream(dst);
      return (int) ByteStreams.copy(stream, out);
    } catch (ErrorResponseException e) {

      if (e.errorResponse().errorCode() == ErrorCode.NO_SUCH_OBJECT) {
        return 0;
      }
      System.out.println(e.errorResponse().errorCode());
      throw new IOException(e);

    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public boolean isOpen() {
    return true;
  }

  @Override
  public void close() throws IOException {
    //
  }

  @Override
  public int write(ByteBuffer src) throws IOException {
    int len = src.remaining();

    try {
      s3.putObject(
          bucket, objName, new ByteBufferBackedInputStream(src), (long) len, null, null, MIME_TYPE);
    } catch (Exception e) {
      throw new IOException(e);
    }
    return len;
  }
}
