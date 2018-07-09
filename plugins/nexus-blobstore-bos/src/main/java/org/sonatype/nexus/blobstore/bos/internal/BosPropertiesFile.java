package org.sonatype.nexus.blobstore.bos.internal;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.baidubce.BceServiceException;
import com.baidubce.services.bos.BosClient;
import com.baidubce.services.bos.model.BosObject;
import com.baidubce.services.bos.model.ObjectMetadata;

import static com.google.common.base.Preconditions.checkNotNull;

public class BosPropertiesFile extends Properties {

    private static final Logger log = LoggerFactory.getLogger(BosPropertiesFile.class);

    private final BosClient bosClient;

    private final String bucket;

    private final String key;


    public BosPropertiesFile(BosClient bosClient, String bucket, String key) {
        this.bosClient = checkNotNull(bosClient);
        this.bucket = checkNotNull(bucket);
        this.key = checkNotNull(key);
    }


    public void load() throws IOException {
        log.debug("Loading: {}/{}", bucket, key);
        try (BosObject bosObject = bosClient.getObject(bucket, key)) {
            try (InputStream inputStream = bosObject.getObjectContent()) {
                load(inputStream);
            }
        }
    }

    public void store() throws IOException {
        log.debug("Storing: {}/{}", bucket, key);

        ByteArrayOutputStream bufferStream = new ByteArrayOutputStream();
        store(bufferStream, null);
        byte[] buffer = bufferStream.toByteArray();

        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentLength(buffer.length);
        bosClient.putObject(bucket, key, new ByteArrayInputStream(buffer), metadata);
    }

    public boolean exists() throws IOException {
        try {
            bosClient.getObjectMetadata(bucket, key);
            return true;
        } catch (BceServiceException e) {
            return false;
        }
    }

    public void remove() throws IOException {
        bosClient.deleteObject(bucket, key);
    }

    public String toString() {
        return getClass().getSimpleName() + "{" +
                "bucket=" + bucket +
                ", key=" + key +
                '}';
    }

    @Override
    public boolean equals(Object object) {
        if (object instanceof BosPropertiesFile) {
            BosPropertiesFile other = (BosPropertiesFile) object;
            return
                    bosClient.equals(other.bosClient) &&
                            bucket.equals(other.bucket) &&
                            key.equals(other.key) &&
                            super.equals(object);
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return bosClient.hashCode() +
                bucket.hashCode() +
                key.hashCode() +
                super.hashCode();
    }
}
