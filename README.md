# BlockIO: A fixed block size based IO library

`BlockIO` provides a random access -like  interface to fixed size devices, like objects stores.

## Example

```java
package org.dcache.blockio;

import io.minio.MinioClient;
import io.minio.errors.MinioException;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class Main {

  public static void main(String args[]) throws IOException, MinioException {

    MinioClient s3 = new MinioClient("https://.../", '"accesskey", "secretKey");

      // block size to use
      int pageSize = 8 * 1024 * 1024;

      // create page cache with max 64 pages ( 64 x 8MB)
      // page supplier will create a new S3-backed page when needed. Per-page s3 object will be stored
      // in the provided bucket and have name 'x-block-<page number>'
      PageIo pageCache = new PageIo(64, pageSize, l -> new Page(ByteBuffer.allocate(pageSize), new S3Block(s3, "data", "x-block-" + l)));

      try (FileChannel in = FileChannel.open(new File("....").toPath())) {

        ByteBuffer b = ByteBuffer.allocate(1024 * 1024);

        long offset = 0;
        while (true) {
          b.clear();
          int n = in.read(b, offset);
          if (n <= 0) {
            break;
          }

          b.flip();
          pageCache.write(offset, b);
          offset += n;
        }
      pageCache.flushAll();
    }
  }
}
```

## License

licensed under [LGPLv2](http://www.gnu.org/licenses/lgpl-2.0.txt "LGPLv2") (or later)

## How to contribute

**BlockIO** uses the linux kernel model of using git not only a source
repository, but also as a way to track contributions and copyrights.

Each submitted patch must have a "Signed-off-by" line.  Patches without
this line will not be accepted.

The sign-off is a simple line at the end of the explanation for the
patch, which certifies that you wrote it or otherwise have the right to
pass it on as an open-source patch.  The rules are pretty simple: if you
can certify the below:

```
Developer's Certificate of Origin 1.1

By making a contribution to this project, I certify that:

(a) The contribution was created in whole or in part by me and I
    have the right to submit it under the open source license
    indicated in the file; or

(b) The contribution is based upon previous work that, to the best
    of my knowledge, is covered under an appropriate open source
    license and I have the right under that license to submit that
    work with modifications, whether created in whole or in part
    by me, under the same open source license (unless I am
    permitted to submit under a different license), as indicated
    in the file; or

(c) The contribution was provided directly to me by some other
    person who certified (a), (b) or (c) and I have not modified
    it.

(d) I understand and agree that this project and the contribution
    are public and that a record of the contribution (including all
    personal information I submit with it, including my sign-off) is
    maintained indefinitely and may be redistributed consistent with
    this project or the open source license(s) involved.
```

then you just add a line saying ( git commit -s )

```
Signed-off-by: Random J Developer <random@developer.example.org>
```

using your real name (sorry, no pseudonyms or anonymous contributions.)