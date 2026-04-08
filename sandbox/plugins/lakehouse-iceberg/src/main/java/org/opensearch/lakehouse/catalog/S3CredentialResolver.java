/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.lakehouse.catalog;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;

import java.io.Closeable;
import java.io.IOException;
import java.util.Locale;

/**
 * Resolves AWS credentials based on the configured credential provider strategy.
 * <ul>
 *   <li><b>default</b> - uses the AWS default credential chain</li>
 *   <li><b>explicit</b> - uses explicitly supplied access key and secret key</li>
 *   <li><b>sts_role</b> - uses STS AssumeRole with the specified role ARN</li>
 * </ul>
 */
public final class S3CredentialResolver {

    static final String PROVIDER_DEFAULT = "default";
    static final String PROVIDER_EXPLICIT = "explicit";
    static final String PROVIDER_STS_ROLE = "sts_role";

    private S3CredentialResolver() {}

    /**
     * Resolves an {@link AwsCredentialsProvider} for the given provider type.
     *
     * @param providerType one of "default", "explicit", or "sts_role"
     * @param accessKey    AWS access key (required for "explicit")
     * @param secretKey    AWS secret key (required for "explicit")
     * @param roleArn      IAM role ARN (required for "sts_role")
     * @param region       AWS region (required for "sts_role")
     * @return the resolved credentials provider
     */
    public static AwsCredentialsProvider resolve(
        String providerType,
        String accessKey,
        String secretKey,
        String roleArn,
        String region
    ) {
        switch (providerType.toLowerCase(Locale.ROOT)) {
            case PROVIDER_DEFAULT:
                return DefaultCredentialsProvider.create();

            case PROVIDER_EXPLICIT:
                if (accessKey == null || secretKey == null) {
                    throw new IllegalArgumentException(
                        "accessKey and secretKey are required for 'explicit' credential provider"
                    );
                }
                return StaticCredentialsProvider.create(
                    AwsBasicCredentials.create(accessKey, secretKey)
                );

            case PROVIDER_STS_ROLE:
                if (roleArn == null) {
                    throw new IllegalArgumentException(
                        "roleArn is required for 'sts_role' credential provider"
                    );
                }
                if (region == null) {
                    throw new IllegalArgumentException(
                        "region is required for 'sts_role' credential provider"
                    );
                }
                StsClient stsClient = StsClient.builder()
                    .region(Region.of(region))
                    .build();
                StsAssumeRoleCredentialsProvider stsProvider = StsAssumeRoleCredentialsProvider.builder()
                    .stsClient(stsClient)
                    .refreshRequest(
                        AssumeRoleRequest.builder()
                            .roleArn(roleArn)
                            .roleSessionName("opensearch-lakehouse-session")
                            .build()
                    )
                    .build();
                return new CloseableStsCredentialsProvider(stsClient, stsProvider);

            default:
                throw new IllegalArgumentException("Unknown credential provider type: " + providerType);
        }
    }

    /**
     * Wraps an {@link StsAssumeRoleCredentialsProvider} and its {@link StsClient}
     * so that both are closed together, preventing the StsClient resource leak.
     */
    static final class CloseableStsCredentialsProvider implements AwsCredentialsProvider, Closeable {
        private final StsClient stsClient;
        private final StsAssumeRoleCredentialsProvider delegate;

        CloseableStsCredentialsProvider(StsClient stsClient, StsAssumeRoleCredentialsProvider delegate) {
            this.stsClient = stsClient;
            this.delegate = delegate;
        }

        @Override
        public AwsCredentials resolveCredentials() {
            return delegate.resolveCredentials();
        }

        @Override
        public void close() throws IOException {
            try {
                delegate.close();
            } finally {
                stsClient.close();
            }
        }
    }
}
