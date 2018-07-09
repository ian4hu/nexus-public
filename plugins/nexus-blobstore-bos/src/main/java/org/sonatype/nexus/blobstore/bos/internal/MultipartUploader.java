package org.sonatype.nexus.blobstore.bos.internal;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import javax.inject.Inject;
import javax.inject.Named;

import org.sonatype.goodies.common.ComponentSupport;
import org.sonatype.nexus.blobstore.api.BlobStoreException;

import com.baidubce.services.bos.BosClient;
import com.baidubce.services.bos.model.AbortMultipartUploadRequest;
import com.baidubce.services.bos.model.CompleteMultipartUploadRequest;
import com.baidubce.services.bos.model.InitiateMultipartUploadRequest;
import com.baidubce.services.bos.model.PartETag;
import com.baidubce.services.bos.model.UploadPartRequest;

@Named
public class MultipartUploader extends ComponentSupport implements BosUploader {

    private final int chunkSize;

    @Inject
    public MultipartUploader(@Named("${nexus.bos.multipartupload.chunksize:-5242880}") final int chunkSize) {
        this.chunkSize = chunkSize;
    }

    @Override
    public void upload(BosClient bosClient, String bucket, String key, InputStream contents) {
        InitiateMultipartUploadRequest initiateRequest = new InitiateMultipartUploadRequest(bucket, key);
        String uploadId = bosClient.initiateMultipartUpload(initiateRequest).getUploadId();

        log.debug("Starting upload {} to key {} in bucket {}", uploadId, key, bucket);

        try (InputStream input = contents) {
            List<PartETag> results = new ArrayList<>();
            for (int partNumber = 1; ; partNumber++) {
                InputStream chunk = readChunk(input);
                if (chunk == null && partNumber > 1) {
                    break;
                } else {
                    // must provide a zero sized chunk if contents is empty
                    if (chunk == null) {
                        chunk = new ByteArrayInputStream(new byte[0]);
                    }
                    log.debug("Uploading chunk {} for {} of {} bytes", partNumber, uploadId, chunk.available());
                    UploadPartRequest part = new UploadPartRequest()
                            .withBucketName(bucket)
                            .withKey(key)
                            .withUploadId(uploadId)
                            .withPartNumber(partNumber)
                            .withInputStream(chunk)
                            .withPartSize(chunk.available());
                    results.add(bosClient.uploadPart(part).getPartETag());
                }
            }
            CompleteMultipartUploadRequest compRequest = new CompleteMultipartUploadRequest()
                    .withBucketName(bucket)
                    .withKey(key)
                    .withUploadId(uploadId)
                    .withPartETags(results);
            bosClient.completeMultipartUpload(compRequest);
            log.debug("Upload {} complete", uploadId);
        } catch (Exception e) {
            try {
                bosClient.abortMultipartUpload(new AbortMultipartUploadRequest(bucket, key, uploadId));
            } catch (Exception abortException) {
                log.error("Error aborting S3 multipart upload to bucket {} with key {}", bucket, key,
                        log.isDebugEnabled() ? abortException : null);
            }
            throw new BlobStoreException("Error uploading blob", e, null);
        }
    }


    private InputStream readChunk(final InputStream input) throws IOException {
        byte[] buffer = new byte[chunkSize];
        int offset = 0;
        int remain = chunkSize;
        int bytesRead = 0;

        while (remain > 0 && bytesRead >= 0) {
            bytesRead = input.read(buffer, offset, remain);
            if (bytesRead > 0) {
                offset += bytesRead;
                remain -= bytesRead;
            }
        }
        if (offset > 0) {
            return new ByteArrayInputStream(buffer, 0, offset);
        } else {
            return null;
        }
    }
}
