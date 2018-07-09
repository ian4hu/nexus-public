package org.sonatype.nexus.blobstore.bos.internal;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.locks.Lock;
import java.util.stream.Stream;

import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Named;

import org.joda.time.DateTime;
import org.sonatype.nexus.blobstore.BlobIdLocationResolver;
import org.sonatype.nexus.blobstore.BlobSupport;
import org.sonatype.nexus.blobstore.MetricsInputStream;
import org.sonatype.nexus.blobstore.StreamMetrics;
import org.sonatype.nexus.blobstore.api.Blob;
import org.sonatype.nexus.blobstore.api.BlobAttributes;
import org.sonatype.nexus.blobstore.api.BlobId;
import org.sonatype.nexus.blobstore.api.BlobMetrics;
import org.sonatype.nexus.blobstore.api.BlobStore;
import org.sonatype.nexus.blobstore.api.BlobStoreConfiguration;
import org.sonatype.nexus.blobstore.api.BlobStoreException;
import org.sonatype.nexus.blobstore.api.BlobStoreMetrics;
import org.sonatype.nexus.blobstore.api.BlobStoreUsageChecker;
import org.sonatype.nexus.common.log.DryRunPrefix;
import org.sonatype.nexus.common.stateguard.Guarded;
import org.sonatype.nexus.common.stateguard.StateGuardLifecycleSupport;

import com.baidubce.http.Headers;
import com.baidubce.services.bos.BosClient;
import com.baidubce.services.bos.model.BosObject;
import com.baidubce.services.bos.model.BosObjectSummary;
import com.baidubce.services.bos.model.ObjectMetadata;
import com.baidubce.util.HttpUtils;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableMap;
import com.google.common.hash.HashCode;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.cache.CacheLoader.from;
import static java.lang.String.format;
import static java.util.stream.StreamSupport.stream;
import static org.sonatype.nexus.blobstore.DirectPathLocationStrategy.DIRECT_PATH_ROOT;
import static org.sonatype.nexus.blobstore.api.BlobAttributesConstants.HEADER_PREFIX;
import static org.sonatype.nexus.common.stateguard.StateGuardLifecycleSupport.State.FAILED;
import static org.sonatype.nexus.common.stateguard.StateGuardLifecycleSupport.State.NEW;
import static org.sonatype.nexus.common.stateguard.StateGuardLifecycleSupport.State.STARTED;
import static org.sonatype.nexus.common.stateguard.StateGuardLifecycleSupport.State.STOPPED;

@Named(BosBlobStore.TYPE)
public class BosBlobStore
        extends StateGuardLifecycleSupport
        implements BlobStore {
    public static final String TYPE = "bos";

    public static final String CONFIG_KEY = "bos";

    public static final String ACCESS_KEY_ID_KEY = "ak";

    public static final String SECRET_ACCESS_KEY_KEY = "sk";

    public static final String ENDPOINT_KEY = "endpoint";

    public static final String PROTOCOL_KEY = "protocol";

    public static final String REGION_KEY = "region";

    public static final String BUCKET_KEY = "bucket";

    public static final String TYPE_KEY = "type";

    public static final String TYPE_V1 = "bos/1";

    public static final String METADATA_FILENAME = "metadata.properties";

    public static final String CONTENT_PREFIX = "maven2";

    public static final String EXPIRATION_KEY = "expiration";

    public static final String BLOB_ATTRIBUTE_SUFFIX = ".properties";

    public static final int DEFAULT_EXPIRATION_IN_DAYS = 3;

    public static final String DIRECT_PATH_PREFIX = CONTENT_PREFIX + "/" + DIRECT_PATH_ROOT;

    private static final String FILE_V1 = "file/1";

    private final BosClientFactory bosClientFactory;

    private final BlobIdLocationResolver blobIdLocationResolver;

    private final BosUploader uploader;

    private final BosBlobStoreMetricsStore storeMetrics;

    private final DryRunPrefix dryRunPrefix;

    private BosClient bosClient;

    private LoadingCache<BlobId, BosBlob> liveBlobs;

    private BlobStoreConfiguration blobStoreConfiguration;

    private interface BlobIngester {
        StreamMetrics ingestTo(final String destination) throws IOException;
    }

    @Inject
    public BosBlobStore(
            final BosClientFactory bosClientFactory,
            final BlobIdLocationResolver blobIdLocationResolver,
            final BosUploader bosUploader,
            final BosBlobStoreMetricsStore storeMetrics,
            final DryRunPrefix dryRunPrefix
    ) {
        this.bosClientFactory = checkNotNull(bosClientFactory);
        this.blobIdLocationResolver = checkNotNull(blobIdLocationResolver);
        this.uploader = checkNotNull(bosUploader);
        this.storeMetrics = checkNotNull(storeMetrics);
        this.dryRunPrefix = checkNotNull(dryRunPrefix);
    }

    @Override
    protected void doStart() throws Exception {
        // ensure blobstore is supported
        BosPropertiesFile metadata = new BosPropertiesFile(bosClient, getConfiguredBucket(), METADATA_FILENAME);
        if (metadata.exists()) {
            metadata.load();
            String type = metadata.getProperty(TYPE_KEY);
            checkState(TYPE_V1.equals(type) || FILE_V1.equals(type), "Unsupported blob store type/version: %s in %s", type, metadata);
        } else {
            // assumes new blobstore, write out type
            metadata.setProperty(TYPE_KEY, TYPE_V1);
            metadata.store();
        }
        liveBlobs = CacheBuilder.newBuilder().weakValues().build(from(BosBlob::new));
        storeMetrics.setBucket(getConfiguredBucket());
        storeMetrics.setBosClient(bosClient);
        storeMetrics.start();
    }

    @Override
    protected void doStop() throws Exception {
        liveBlobs = null;
        storeMetrics.stop();
    }

    /**
     * Returns path for blob-id content file relative to root directory.
     */
    private String contentPath(final BlobId id) {
        return getLocation(id);
    }

    /**
     * Returns path for blob-id attribute file relative to root directory.
     */
    private String attributePath(final BlobId id) {
        return getLocation(id) + BLOB_ATTRIBUTE_SUFFIX;
    }


    /**
     * Returns the location for a blob ID based on whether or not the blob ID is for a temporary or permanent blob.
     */
    private String getLocation(final BlobId id) {
        return CONTENT_PREFIX + "/" + blobIdLocationResolver.getLocation(id);
    }

    @Override
    @Guarded(by = STARTED)
    public Blob create(final InputStream blobData, final Map<String, String> headers) {
        checkNotNull(blobData);

        return create(headers, destination -> {
            try (InputStream data = blobData) {
                MetricsInputStream input = new MetricsInputStream(data);
                uploader.upload(bosClient, getConfiguredBucket(), destination, input);
                return input.getMetrics();
            }
        });
    }

    @Override
    @Guarded(by = STARTED)
    public Blob create(final Path sourceFile, final Map<String, String> headers, final long size, final HashCode sha1) {
        throw new BlobStoreException("hard links not supported", null);
    }

    private Blob create(final Map<String, String> headers, final BlobIngester ingester) {
        checkNotNull(headers);

        checkArgument(headers.containsKey(BLOB_NAME_HEADER), "Missing header: %s", BLOB_NAME_HEADER);
        checkArgument(headers.containsKey(CREATED_BY_HEADER), "Missing header: %s", CREATED_BY_HEADER);
        log.info("BlobHeaders: {}", headers);
        final BlobId blobId = blobIdLocationResolver.fromHeaders(headers);

        final String blobPath = contentPath(blobId);
        final String attributePath = attributePath(blobId);
        final boolean isDirectPath = Boolean.parseBoolean(headers.getOrDefault(DIRECT_PATH_BLOB_HEADER, "false"));
        Long existingSize = null;
        if (isDirectPath) {
            BosBlobAttributes blobAttributes = new BosBlobAttributes(bosClient, getConfiguredBucket(), attributePath);
            if (exists(blobId)) {
                existingSize = getContentSizeForDeletion(blobAttributes);
            }
        }

        final BosBlob blob = liveBlobs.getUnchecked(blobId);

        Lock lock = blob.lock();
        try {
            log.debug("Writing blob {} to {}", blobId, blobPath);

            final StreamMetrics streamMetrics = ingester.ingestTo(blobPath);
            final BlobMetrics metrics = new BlobMetrics(new DateTime(), streamMetrics.getSha1(), streamMetrics.getSize());
            blob.refresh(headers, metrics);

            BosBlobAttributes blobAttributes = new BosBlobAttributes(bosClient, getConfiguredBucket(), attributePath, headers, metrics);

            blobAttributes.store();
            if (isDirectPath && existingSize != null) {
                storeMetrics.recordDeletion(existingSize);
            }
            storeMetrics.recordAddition(blobAttributes.getMetrics().getContentSize());

            return blob;
        } catch (IOException e) {
            // Something went wrong, clean up the files we created
            deleteQuietly(attributePath);
            deleteQuietly(blobPath);
            throw new BlobStoreException(e, blobId);
        } finally {
            lock.unlock();
        }
    }

    @Override
    @Guarded(by = STARTED)
    public Blob copy(final BlobId blobId, final Map<String, String> headers) {
        Blob sourceBlob = checkNotNull(get(blobId));
        String sourcePath = contentPath(sourceBlob.getId());
        return create(headers, destination -> {
            bosClient.copyObject(getConfiguredBucket(), sourcePath, getConfiguredBucket(), destination);
            BlobMetrics metrics = sourceBlob.getMetrics();
            return new StreamMetrics(metrics.getContentSize(), metrics.getSha1Hash());
        });
    }

    @Nullable
    @Override
    @Guarded(by = STARTED)
    public Blob get(final BlobId blobId) {
        return get(blobId, false);
    }

    @Nullable
    @Override
    public Blob get(final BlobId blobId, final boolean includeDeleted) {
        checkNotNull(blobId);

        final BosBlob blob = liveBlobs.getUnchecked(blobId);

        if (blob.isStale()) {
            Lock lock = blob.lock();
            try {
                if (blob.isStale()) {
                    BosBlobAttributes blobAttributes = new BosBlobAttributes(bosClient, getConfiguredBucket(), attributePath(blobId));
                    boolean loaded = blobAttributes.load();
                    if (!loaded) {
                        log.warn("Attempt to access non-existent blob {} ({})", blobId, blobAttributes);
                        return null;
                    }

                    if (blobAttributes.isDeleted() && !includeDeleted) {
                        log.warn("Attempt to access soft-deleted blob {} ({})", blobId, blobAttributes);
                        return null;
                    }

                    blob.refresh(blobAttributes.getHeaders(), blobAttributes.getMetrics());
                }
            } catch (IOException e) {
                throw new BlobStoreException(e, blobId);
            } finally {
                lock.unlock();
            }
        }

        log.debug("Accessing blob {}", blobId);

        return blob;
    }

    @Override
    @Guarded(by = STARTED)
    public boolean delete(final BlobId blobId, String reason) {
        checkNotNull(blobId);

        final BosBlob blob = liveBlobs.getUnchecked(blobId);

        Lock lock = blob.lock();
        try {
            log.debug("Soft deleting blob {}", blobId);

            BosBlobAttributes blobAttributes = new BosBlobAttributes(bosClient, getConfiguredBucket(), attributePath(blobId));

            boolean loaded = blobAttributes.load();
            if (!loaded) {
                // This could happen under some concurrent situations (two threads try to delete the same blob)
                // but it can also occur if the deleted index refers to a manually-deleted blob.
                log.warn("Attempt to mark-for-delete non-existent blob {}", blobId);
                return false;
            } else if (blobAttributes.isDeleted()) {
                log.debug("Attempt to delete already-deleted blob {}", blobId);
                return false;
            }

            blobAttributes.setDeleted(true);
            blobAttributes.setDeletedReason(reason);
            blobAttributes.store();

            // soft delete is implemented using an S3 lifecycle that sets expiration on objects with DELETED_TAG
            // tag the bytes
            // TODO: soft delete
            //bosClient.setObjectTagging(tagAsDeleted(contentPath(blobId)));
            // tag the attributes
            //bosClient.setObjectTagging(tagAsDeleted(attributePath(blobId)));
            blob.markStale();

            return true;
        } catch (Exception e) {
            throw new BlobStoreException(e, blobId);
        } finally {
            lock.unlock();
        }
    }

    @Override
    @Guarded(by = STARTED)
    public boolean deleteHard(final BlobId blobId) {
        checkNotNull(blobId);

        try {
            log.debug("Hard deleting blob {}", blobId);

            String attributePath = attributePath(blobId);
            BosBlobAttributes blobAttributes = new BosBlobAttributes(bosClient, getConfiguredBucket(), attributePath);
            Long contentSize = getContentSizeForDeletion(blobAttributes);

            String blobPath = contentPath(blobId);

            boolean blobDeleted = delete(blobPath);
            delete(attributePath);

            if (blobDeleted && contentSize != null) {
                storeMetrics.recordDeletion(contentSize);
            }

            return blobDeleted;
        } catch (IOException e) {
            throw new BlobStoreException(e, blobId);
        } finally {
            liveBlobs.invalidate(blobId);
        }
    }

    @Nullable
    private Long getContentSizeForDeletion(final BosBlobAttributes blobAttributes) {
        try {
            blobAttributes.load();
            return blobAttributes.getMetrics() != null ? blobAttributes.getMetrics().getContentSize() : null;
        } catch (Exception e) {
            log.warn("Unable to load attributes {}, delete will not be added to metrics.", blobAttributes, e);
            return null;
        }
    }

    @Override
    @Guarded(by = STARTED)
    public BlobStoreMetrics getMetrics() {
        return storeMetrics.getMetrics();
    }

    @Override
    @Guarded(by = STARTED)
    public synchronized void compact() {
        compact(null);
    }

    @Override
    @Guarded(by = STARTED)
    public synchronized void compact(@Nullable final BlobStoreUsageChecker inUseChecker) {
        // no-op
    }

    @Override
    public void init(final BlobStoreConfiguration configuration) {
        this.blobStoreConfiguration = configuration;
        try {
            this.bosClient = bosClientFactory.create(configuration);
            if (!bosClient.doesBucketExist(getConfiguredBucket())) {
                bosClient.createBucket(getConfiguredBucket());
            }
            /*
            if (getConfiguredExpirationInDays() >= 0) {
                // bucket exists, we should test that the correct lifecycle config is present
                // TODO: setup storage strategy
            }*/

        } catch (Exception e) {
            throw new BlobStoreException("Unable to initialize blob store bucket: " + getConfiguredBucket(), e, null);
        }
    }

    private int getConfiguredExpirationInDays() {
        return Integer.parseInt(
                blobStoreConfiguration.attributes(CONFIG_KEY).get(EXPIRATION_KEY, DEFAULT_EXPIRATION_IN_DAYS).toString()
        );
    }


    private boolean delete(final String path) throws IOException {
        bosClient.deleteObject(getConfiguredBucket(), path);
        // note: no info returned from s3
        return true;
    }

    private void deleteQuietly(final String path) {
        bosClient.deleteObject(getConfiguredBucket(), path);
    }


    private String getConfiguredBucket() {
        return blobStoreConfiguration.attributes(CONFIG_KEY).require(BUCKET_KEY, String.class);
    }

    @Override
    public BlobStoreConfiguration getBlobStoreConfiguration() {
        return blobStoreConfiguration;
    }

    /**
     * Delete files known to be part of the S3BlobStore implementation if the content directory is empty.
     */
    @Override
    @Guarded(by = {NEW, STOPPED, FAILED})
    public void remove() {
        try {
            boolean contentEmpty = bosClient.listObjects(getConfiguredBucket(), CONTENT_PREFIX + "/").getContents().isEmpty();
            if (contentEmpty) {
                BosPropertiesFile metadata = new BosPropertiesFile(bosClient, getConfiguredBucket(), METADATA_FILENAME);
                metadata.remove();
                storeMetrics.remove();
                bosClient.deleteBucket(getConfiguredBucket());
            } else {
                log.warn("Unable to delete non-empty blob store content directory in bucket {}", getConfiguredBucket());
            }
        } catch (Exception e) {
            throw new BlobStoreException(e, null);
        }
    }

    @Override
    public Stream<BlobId> getBlobIdStream() {
        Iterable<BosObjectSummary> summaries = BosObjects.withPrefix(bosClient, getConfiguredBucket(), CONTENT_PREFIX);
        return blobIdStream(summaries);
    }

    private Stream<BlobId> blobIdStream(Iterable<BosObjectSummary> summaries) {
        return stream(summaries.spliterator(), false)
                .map(BosObjectSummary::getKey)
                .map(key -> key.substring(key.lastIndexOf('/') + 1, key.length()))
                .filter(filename -> filename.endsWith(BLOB_ATTRIBUTE_SUFFIX))
                .map(filename -> filename.substring(0, filename.length() - BLOB_ATTRIBUTE_SUFFIX.length()))
                .map(BlobId::new);
    }

    @Override
    public Stream<BlobId> getDirectPathBlobIdStream(final String prefix) {
        String subpath = format("%s/%s", DIRECT_PATH_PREFIX, prefix);
        Iterable<BosObjectSummary> summaries = BosObjects.withPrefix(bosClient, getConfiguredBucket(), subpath);
        return stream(summaries.spliterator(), false)
                .map(BosObjectSummary::getKey)
                .filter(key -> key.endsWith(BLOB_ATTRIBUTE_SUFFIX))
                .map(this::attributePathToDirectPathBlobId);
    }

    @Nullable
    @Override
    public BlobAttributes getBlobAttributes(final BlobId blobId) {
        try {
            BosBlobAttributes blobAttributes = new BosBlobAttributes(bosClient, getConfiguredBucket(), attributePath(blobId));
            return blobAttributes.load() ? blobAttributes : null;
        } catch (IOException e) {
            log.error("Unable to load S3BlobAttributes for blob id: {}", blobId, e);
            return null;
        }
    }

    @Override
    public void setBlobAttributes(BlobId blobId, BlobAttributes blobAttributes) {
        try {
            BosBlobAttributes s3BlobAttributes = (BosBlobAttributes) getBlobAttributes(blobId);
            s3BlobAttributes.updateFrom(blobAttributes);
            s3BlobAttributes.store();
        } catch (Exception e) {
            log.error("Unable to set BlobAttributes for blob id: {}, exception: {}",
                    blobId, e.getMessage(), log.isDebugEnabled() ? e : null);
        }
    }

    @Override
    public boolean undelete(@Nullable final BlobStoreUsageChecker inUseChecker,
            final BlobId blobId,
            final BlobAttributes attributes,
            final boolean isDryRun) {
        checkNotNull(attributes);
        String logPrefix = isDryRun ? dryRunPrefix.get() : "";
        Optional<String> blobName = Optional.of(attributes)
                .map(BlobAttributes::getProperties)
                .map(p -> p.getProperty(HEADER_PREFIX + BLOB_NAME_HEADER));
        if (!blobName.isPresent()) {
            log.error("Property not present: {}, for blob id: {}, at path: {}", HEADER_PREFIX + BLOB_NAME_HEADER,
                    blobId, attributePath(blobId));
            return false;
        }
        if (attributes.isDeleted() && inUseChecker != null && inUseChecker.test(this, blobId, blobName.get())) {
            String deletedReason = attributes.getDeletedReason();
            if (!isDryRun) {
                attributes.setDeleted(false);
                attributes.setDeletedReason(null);
                try {
                    // TODO: soft delete
                    //s3.setObjectTagging(untagAsDeleted(contentPath(blobId)));
                    //s3.setObjectTagging(untagAsDeleted(attributePath(blobId)));
                    attributes.store();
                } catch (IOException e) {
                    log.error("Error while un-deleting blob id: {}, deleted reason: {}, blob store: {}, blob name: {}",
                            blobId, deletedReason, blobStoreConfiguration.getName(), blobName.get(), e);
                }
            }
            log.warn(
                    "{}Soft-deleted blob still in use, un-deleting blob id: {}, deleted reason: {}, blob store: {}, blob name: {}",
                    logPrefix, blobId, deletedReason, blobStoreConfiguration.getName(), blobName.get());
            return true;
        }
        return false;
    }

    /**
     * This is a simple existence check resulting from NEXUS-16729.  This allows clients
     * to perform a simple check primarily intended for use in directpath scenarios.
     */
    @Override
    public boolean exists(final BlobId blobId) {
        checkNotNull(blobId);
        BosBlobAttributes blobAttributes = new BosBlobAttributes(bosClient, getConfiguredBucket(), attributePath(blobId));
        try {
            return blobAttributes.load();
        } catch (IOException ioe) {
            log.debug("Unable to load attributes {} during existence check, exception: {}", blobAttributes, ioe);
            return false;
        }
    }

    /**
     * Used by {@link #getDirectPathBlobIdStream(String)} to convert an s3 key to a {@link BlobId}.
     *
     * @see BlobIdLocationResolver
     */
    private BlobId attributePathToDirectPathBlobId(final String s3Key) { // NOSONAR
        checkArgument(s3Key.startsWith(DIRECT_PATH_PREFIX + "/"), "Not direct path blob path: %s", s3Key);
        checkArgument(s3Key.endsWith(BLOB_ATTRIBUTE_SUFFIX), "Not blob attribute path: %s", s3Key);
        String blobName = s3Key
                .substring(0, s3Key.length() - BLOB_ATTRIBUTE_SUFFIX.length())
                .substring(DIRECT_PATH_PREFIX.length() + 1);
        Map<String, String> headers = ImmutableMap.of(
                BLOB_NAME_HEADER, blobName,
                DIRECT_PATH_BLOB_HEADER, "true"
        );
        return blobIdLocationResolver.fromHeaders(headers);
    }

    class BosBlob extends BlobSupport {
        public BosBlob(BlobId blobId) {
            super(blobId);
        }

        @Override
        public InputStream getInputStream() {
            BosObject object = bosClient.getObject(getConfiguredBucket(), contentPath(getId()));
            return object.getObjectContent();
        }

        @Override
        public Map<String, String> getHeaders() {
            ObjectMetadata metadata = bosClient.getObjectMetadata(getConfiguredBucket(), contentPath(getId()));
            HashMap<String, String> headers = new HashMap<>();
            if (metadata.getContentType() != null) {
                headers.put(Headers.CONTENT_TYPE, metadata.getContentType());
            }
            if (metadata.getContentMd5() != null) {
                headers.put(Headers.CONTENT_MD5, metadata.getContentMd5());
            }
            if (metadata.getContentEncoding() != null) {
                headers.put(Headers.CONTENT_ENCODING, metadata.getContentEncoding());
            }
            if (metadata.getBceContentSha256() != null) {
                headers.put(Headers.BCE_CONTENT_SHA256, metadata.getBceContentSha256());
            }
            if (metadata.getContentDisposition() != null) {
                headers.put(Headers.CONTENT_DISPOSITION, metadata.getContentDisposition());
            }
            if (metadata.getETag() != null) {
                headers.put(Headers.ETAG, metadata.getETag());
            }
            if (metadata.getExpires() != null) {
                headers.put(Headers.EXPIRES, metadata.getExpires());
            }
            if (metadata.getCacheControl() != null) {
                headers.put(Headers.CACHE_CONTROL, metadata.getCacheControl());
            }
            if (metadata.getStorageClass() != null) {
                headers.put(Headers.BCE_STORAGE_CLASS, metadata.getStorageClass());
            }

            Map<String, String> userMetadata = metadata.getUserMetadata();
            if (userMetadata != null) {
                for (Map.Entry<String, String> entry : userMetadata.entrySet()) {
                    String key = entry.getKey();
                    if (key == null) {
                        continue;
                    }
                    String value = entry.getValue();
                    if (value == null) {
                        value = "";
                    }
                    headers.put(Headers.BCE_USER_METADATA_PREFIX + HttpUtils.normalize(key.trim()),
                            HttpUtils.normalize(value));
                }
            }
            return headers;
        }
    }

}
