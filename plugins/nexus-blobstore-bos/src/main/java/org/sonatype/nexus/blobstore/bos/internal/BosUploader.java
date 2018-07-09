package org.sonatype.nexus.blobstore.bos.internal;

import java.io.InputStream;

import com.baidubce.services.bos.BosClient;

public interface BosUploader {
    void upload(BosClient bosClient, String bucket, String key, InputStream contents);
}
