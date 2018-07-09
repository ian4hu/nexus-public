package org.sonatype.nexus.blobstore.bos.internal;

import java.util.Iterator;


import com.baidubce.services.bos.BosClient;
import com.baidubce.services.bos.model.BosObjectSummary;
import com.baidubce.services.bos.model.ListObjectsRequest;
import com.baidubce.services.bos.model.ListObjectsResponse;

public class BosObjects implements Iterable<BosObjectSummary> {

    private BosClient bosClient;

    private String prefix = null;

    private String bucketName;

    private Integer batchSize = null;

    private BosObjects(BosClient bosClient, String bucketName) {
        this.bosClient = bosClient;
        this.bucketName = bucketName;
    }

    /**
     * Constructs an iterable that covers all the objects in an Amazon S3
     * bucket.
     *
     * @param bosClient
     *            The Amazon S3 client.
     * @param bucketName
     *            The bucket name.
     * @return An iterator for object summaries.
     */
    public static BosObjects inBucket(BosClient bosClient, String bucketName) {
        return new BosObjects(bosClient, bucketName);
    }

    /**
     * Constructs an iterable that covers the objects in an Amazon S3 bucket
     * where the key begins with the given prefix.
     *
     * @param bosClient
     *            The Amazon S3 client.
     * @param bucketName
     *            The bucket name.
     * @param prefix
     *            The prefix.
     * @return An iterator for object summaries.
     */
    public static BosObjects withPrefix(BosClient bosClient, String bucketName, String prefix) {
        BosObjects objects = new BosObjects(bosClient, bucketName);
        objects.prefix = prefix;
        return objects;
    }

    /**
     * Sets the batch size, i.e., how many {@link S3ObjectSummary}s will be
     * fetched at once.
     *
     * @param batchSize
     *            How many object summaries to fetch at once.
     */
    public BosObjects withBatchSize(int batchSize) {
        this.batchSize = batchSize;
        return this;
    }

    public Integer getBatchSize() {
        return batchSize;
    }

    public String getPrefix() {
        return prefix;
    }

    public String getBucketName() {
        return bucketName;
    }

    public BosClient getBosClient() {
        return bosClient;
    }

    private class BosObjectIterator implements Iterator<BosObjectSummary> {

        private ListObjectsResponse currentListing = null;

        private Iterator<BosObjectSummary> currentIterator = null;

        @Override
        public boolean hasNext() {
            prepareCurrentListing();
            return currentIterator.hasNext();
        }

        @Override
        public BosObjectSummary next() {
            prepareCurrentListing();
            return currentIterator.next();
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

        private void prepareCurrentListing() {
            while (currentListing == null || (!currentIterator.hasNext() && currentListing.isTruncated())) {

                if (currentListing == null) {
                    ListObjectsRequest req = new ListObjectsRequest(getBucketName());
                    req.setBucketName(getBucketName());
                    req.setPrefix(getPrefix());
                    req.setMaxKeys(getBatchSize());
                    currentListing = getBosClient().listObjects(req);
                } else {
                    currentListing = getBosClient().listNextBatchOfObjects(currentListing);
                }

                currentIterator = currentListing.getContents().iterator();
            }
        }

    }

    @Override
    public Iterator<BosObjectSummary> iterator() {
        return new BosObjectIterator();
    }

}

