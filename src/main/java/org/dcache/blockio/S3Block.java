package org.dcache.blockio;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.S3Object;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ByteChannel;

/**
 *
 */
public class S3Block implements ByteChannel {

    private final static String MIME_TYPE = "binary/octet-stream";

    private final AmazonS3 s3;
    private final String bucket;
    private final String objName;


    public S3Block(AmazonS3 s3, String bucket, String objName) {
        this.s3 = s3;
        this.bucket = bucket;
        this.objName = objName;
    }

    @Override
    public int read(ByteBuffer dst) throws IOException {

        int len = dst.remaining();
        try {
            S3Object o = s3.getObject(bucket, objName);
            len = (int) o.getObjectMetadata().getContentLength();

            o.getObjectContent().read(dst.array(), 0, len);
        } catch (AmazonS3Exception e) {

        }
        return len;
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
        ObjectMetadata om = new ObjectMetadata();
        om.setContentLength(len);
        om.setContentType(MIME_TYPE);
        PutObjectResult r = s3.putObject(bucket, objName, new ByteArrayInputStream(src.array(), 0, src.remaining()), om);
        return len;
    }

}
