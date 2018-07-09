package org.sonatype.nexus.blobstore.bos.internal;

import java.util.Arrays;
import java.util.List;

import javax.inject.Named;

import org.sonatype.nexus.blobstore.BlobStoreDescriptor;
import org.sonatype.nexus.formfields.FormField;
import org.sonatype.nexus.formfields.PasswordFormField;
import org.sonatype.nexus.formfields.StringTextFormField;

@Named(BosBlobStoreDescriptor.TYPE)
public class BosBlobStoreDescriptor implements BlobStoreDescriptor {

    public static final String TYPE = "bos";

    private final FormField<String> bucket;

    private final FormField<String> ak;

    private final FormField<String> sk;

    private final FormField<String> endpoint;

    private final FormField<String> region;

    public BosBlobStoreDescriptor() {
        bucket = new StringTextFormField(
                BosBlobStore.BUCKET_KEY,
                "Bucket",
                "Baidu BOS bucket",
                true
        );
        ak = new StringTextFormField(
                BosBlobStore.ACCESS_KEY_ID_KEY,
                "AK",
                "Baidu BOS AK",
                true
        );
        sk = new PasswordFormField(
                BosBlobStore.SECRET_ACCESS_KEY_KEY,
                "SK",
                "Baidu BOS sk",
                true
        );
        endpoint = new StringTextFormField(
                BosBlobStore.ENDPOINT_KEY,
                "Endpoint",
                "Bos endpoint",
                true
        );
        region = new StringTextFormField(
                BosBlobStore.REGION_KEY,
                "Region",
                "Bos region",
                false
        );
    }

    @Override
    public String getName() {
        return TYPE;
    }

    @Override
    public List<FormField> getFormFields() {
        return Arrays.asList(
                bucket, ak, sk, endpoint, region
        );
    }
}
