/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.lakehouse.catalog;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.core.exception.SdkClientException;

import java.util.Map;

/**
 * A ThreadLocal-based {@link AwsCredentialsProvider} for the Iceberg SDK.
 *
 * <p>The Iceberg SDK instantiates this class by name via reflection (from the
 * {@code client.credentials-provider} catalog property). It calls
 * {@link #resolveCredentials()} each time it needs credentials for an AWS API call.
 *
 * <p>Calling code sets per-catalog credentials on the current thread's ThreadLocal
 * before making Iceberg SDK calls, and clears them in a finally block.
 */
public class LakehouseCredentialsProvider implements AwsCredentialsProvider {

    private static final ThreadLocal<AwsCredentials> CURRENT = new ThreadLocal<>();

    /** No-arg constructor. */
    public LakehouseCredentialsProvider() {}

    /**
     * Static factory method required by Iceberg SDK's {@code AwsClientProperties}.
     *
     * @param properties catalog properties (unused — credentials come from ThreadLocal)
     * @return a new provider instance
     */
    public static LakehouseCredentialsProvider create(Map<String, String> properties) {
        return new LakehouseCredentialsProvider();
    }

    /**
     * No-arg static factory fallback for Iceberg SDK.
     *
     * @return a new provider instance
     */
    public static LakehouseCredentialsProvider create() {
        return new LakehouseCredentialsProvider();
    }

    /**
     * Sets credentials on the current thread. Must be paired with {@link #clear()} in a finally block.
     *
     * @param creds the per-catalog credentials
     */
    public static void set(AwsCredentials creds) {
        CURRENT.set(creds);
    }

    /**
     * Returns the credentials on the current thread, or null if none set.
     *
     * @return current thread credentials, or null
     */
    public static AwsCredentials get() {
        return CURRENT.get();
    }

    /**
     * Clears the current thread's credentials. Always call in a finally block.
     */
    public static void clear() {
        CURRENT.remove();
    }

    @Override
    public software.amazon.awssdk.auth.credentials.AwsCredentials resolveCredentials() {
        AwsCredentials creds = CURRENT.get();
        if (creds == null || !creds.isComplete()) {
            throw SdkClientException.create(
                "No credentials set on current thread. " + "Call LakehouseCredentialsProvider.set() before making Iceberg SDK calls."
            );
        }
        if (creds.getSessionToken() != null) {
            return AwsSessionCredentials.create(creds.getAccessKeyId(), creds.getSecretAccessKey(), creds.getSessionToken());
        }
        return AwsBasicCredentials.create(creds.getAccessKeyId(), creds.getSecretAccessKey());
    }
}
