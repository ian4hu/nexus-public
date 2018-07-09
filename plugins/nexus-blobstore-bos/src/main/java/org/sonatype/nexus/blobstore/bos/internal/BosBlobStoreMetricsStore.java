package org.sonatype.nexus.blobstore.bos.internal;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import javax.inject.Inject;
import javax.inject.Named;

import org.sonatype.nexus.blobstore.AccumulatingBlobStoreMetrics;
import org.sonatype.nexus.blobstore.PeriodicJobService;
import org.sonatype.nexus.blobstore.api.BlobStoreConfiguration;
import org.sonatype.nexus.blobstore.api.BlobStoreMetrics;
import org.sonatype.nexus.common.node.NodeAccess;
import org.sonatype.nexus.common.stateguard.Guarded;
import org.sonatype.nexus.common.stateguard.StateGuardLifecycleSupport;

import com.baidubce.services.bos.BosClient;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.Long.parseLong;
import static org.sonatype.nexus.common.stateguard.StateGuardLifecycleSupport.State.STARTED;

@Named
public class BosBlobStoreMetricsStore extends StateGuardLifecycleSupport {

    private static final String METRICS_SUFFIX = "-metrics";

    private static final String METRICS_EXTENSION = ".properties";

    private static final int METRICS_FLUSH_PERIOD_SECONDS = 10;

    private static final String TOTAL_SIZE_PROP_NAME = "totalSize";

    private static final String BLOB_COUNT_PROP_NAME = "blobCount";

    private final PeriodicJobService jobService;

    private AtomicLong blobCount;

    private final NodeAccess nodeAccess;

    private AtomicLong totalSize;

    private AtomicBoolean dirty;

    private PeriodicJobService.PeriodicJob metricsWritingJob;

    private String bucket;

    private BosPropertiesFile propertiesFile;

    private BosClient bosClient;

    private BlobStoreConfiguration blobStoreConfiguration;


    @Inject
    public BosBlobStoreMetricsStore(PeriodicJobService jobService, NodeAccess nodeAccess) {
        this.jobService = jobService;
        this.nodeAccess = nodeAccess;
    }

    @Override
    protected void doStart() throws Exception {
        blobCount = new AtomicLong();
        totalSize = new AtomicLong();
        dirty = new AtomicBoolean();

        propertiesFile = new BosPropertiesFile(bosClient, bucket, nodeAccess.getId() + METRICS_SUFFIX + METRICS_EXTENSION);
        if (propertiesFile.exists()) {
            log.info("Loading blob store metrics file {}", propertiesFile);
            propertiesFile.load();
            readProperties();
        } else {
            log.info("Blob store metrics file {} not found - initializing at zero.", propertiesFile);
            updateProperties();
            propertiesFile.store();
        }

        jobService.startUsing();
        metricsWritingJob = jobService.schedule(() -> {
            try {
                if (dirty.compareAndSet(true, false)) {
                    updateProperties();
                    log.trace("Writing blob store metrics to {}", propertiesFile);
                    propertiesFile.store();
                }
            } catch (Exception e) {
                // Don't propagate, as this stops subsequent executions
                log.error("Cannot write blob store metrics", e);
            }
        }, METRICS_FLUSH_PERIOD_SECONDS);
    }

    @Override
    protected void doStop() throws Exception {
        metricsWritingJob.cancel();
        metricsWritingJob = null;
        jobService.stopUsing();

        blobCount = null;
        totalSize = null;
        dirty = null;

        propertiesFile = null;
    }

    public void setBucket(final String bucket) {
        checkState(this.bucket == null, "Do not initialize twice");
        checkNotNull(bucket);
        this.bucket = bucket;
    }

    public void setBosClient(final BosClient bosClient) {
        checkState(this.bosClient == null, "Do not initialize twice");
        checkNotNull(bosClient);
        this.bosClient = bosClient;
    }

    @Guarded(by = STARTED)
    public BlobStoreMetrics getMetrics() {
        Stream<BosPropertiesFile> blobStoreMetricsFiles = backingFiles();
        return getCombinedMetrics(blobStoreMetricsFiles);
    }

    private BlobStoreMetrics getCombinedMetrics(final Stream<BosPropertiesFile> blobStoreMetricsFiles) {
        AccumulatingBlobStoreMetrics blobStoreMetrics = new AccumulatingBlobStoreMetrics(0, 0, -1, true);

        blobStoreMetricsFiles.forEach(metricsFile -> {
            try {
                metricsFile.load();
                blobStoreMetrics.addBlobCount(parseLong(metricsFile.getProperty(BLOB_COUNT_PROP_NAME, "0")));
                blobStoreMetrics.addTotalSize(parseLong(metricsFile.getProperty(TOTAL_SIZE_PROP_NAME, "0")));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        return blobStoreMetrics;
    }

    @Guarded(by = STARTED)
    public void recordAddition(final long size) {
        blobCount.incrementAndGet();
        totalSize.addAndGet(size);
        dirty.set(true);
    }

    @Guarded(by = STARTED)
    public void recordDeletion(final long size) {
        blobCount.decrementAndGet();
        totalSize.addAndGet(-size);
        dirty.set(true);
    }

    public void remove() {
        backingFiles().forEach(metricsFile -> {
            try {
                metricsFile.remove();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    private Stream<BosPropertiesFile> backingFiles() {
        if (bosClient == null) {
            return Stream.empty();
        } else {
            Stream<BosPropertiesFile> stream = bosClient.listObjects(bucket, nodeAccess.getId()).getContents().stream()
                    .filter(summary -> summary.getKey().endsWith(METRICS_EXTENSION))
                    .map(summary -> new BosPropertiesFile(bosClient, bucket, summary.getKey()));
            return stream;
        }
    }

    private void updateProperties() {
        propertiesFile.setProperty(TOTAL_SIZE_PROP_NAME, totalSize.toString());
        propertiesFile.setProperty(BLOB_COUNT_PROP_NAME, blobCount.toString());
    }

    private void readProperties() {
        String size = propertiesFile.getProperty(TOTAL_SIZE_PROP_NAME);
        if (size != null) {
            totalSize.set(parseLong(size));
        }

        String count = propertiesFile.getProperty(BLOB_COUNT_PROP_NAME);
        if (count != null) {
            blobCount.set(parseLong(count));
        }
    }

}
