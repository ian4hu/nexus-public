package org.sonatype.nexus.blobstore.bos.internal;

import javax.inject.Named;

import org.sonatype.goodies.common.ComponentSupport;
import org.sonatype.nexus.blobstore.api.BlobStoreConfiguration;
import org.sonatype.nexus.common.collect.NestedAttributesMap;

import com.baidubce.Protocol;
import com.baidubce.Region;
import com.baidubce.auth.DefaultBceCredentials;
import com.baidubce.services.bos.BosClient;
import com.baidubce.services.bos.BosClientConfiguration;

import static com.google.common.base.Strings.isNullOrEmpty;
import static org.sonatype.nexus.blobstore.bos.internal.BosBlobStore.*;

@Named
public class BosClientFactory extends ComponentSupport {

    public BosClient create(final BlobStoreConfiguration blobStoreConfiguration) {
        BosClientConfiguration bosConf = new BosClientConfiguration();
        NestedAttributesMap attributes = blobStoreConfiguration.attributes(CONFIG_KEY);
        String ak = attributes.get(ACCESS_KEY_ID_KEY, String.class);
        String sk = attributes.get(SECRET_ACCESS_KEY_KEY, String.class);
        String endpoint = attributes.get(ENDPOINT_KEY, String.class);
        String protocol = attributes.get(PROTOCOL_KEY, String.class);
        String region = attributes.get(REGION_KEY, String.class);
        bosConf.withEndpoint(endpoint);
        if (!isNullOrEmpty(protocol)) {
            bosConf.withProtocol(parseProtocol(protocol));
        }
        if (!isNullOrEmpty(region)) {
            bosConf.withRegion(Region.fromValue(region));
        }
        if (!isNullOrEmpty(ak) && !isNullOrEmpty(sk)) {
            bosConf.withCredentials(new DefaultBceCredentials(ak, sk));
        } else {
            throw new IllegalArgumentException("AK or SK is missing");
        }

        return new BosClient(bosConf);
    }

    private static Protocol parseProtocol(String protocol) {
        for (Protocol value : Protocol.values()) {
            if (value.toString().equals(protocol)) {
                return value;
            }
        }
        throw new IllegalArgumentException("'" + protocol + "' is not a valid protocol.");
    }
}
