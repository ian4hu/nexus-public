package org.sonatype.nexus.blobstore.bos.internal;

import java.io.IOException;
import java.util.Map;

import javax.annotation.Nullable;

import org.sonatype.nexus.blobstore.BlobAttributesSupport;
import org.sonatype.nexus.blobstore.api.BlobMetrics;

import com.baidubce.services.bos.BosClient;

public class BosBlobAttributes extends BlobAttributesSupport<BosPropertiesFile> {
    public BosBlobAttributes(final BosClient bosClient, final String bucket, final String key, @Nullable Map<String, String> headers, @Nullable BlobMetrics metrics) {
        super(new BosPropertiesFile(bosClient, bucket, key), headers, metrics);
    }

    public BosBlobAttributes(final BosClient bosClient, final String bucket, final String key) {
        super(new BosPropertiesFile(bosClient, bucket, key), null, null);
    }

    @Override
    public void store() throws IOException {
        writeTo(propertiesFile);
        propertiesFile.store();
    }

    public boolean load() throws IOException {
        if (!propertiesFile.exists()) {
            return false;
        }
        propertiesFile.load();
        readFrom(propertiesFile);
        return true;
    }
}
