/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.lakehouse.catalog;

/**
 * Holds resolved AWS credentials for a specific catalog.
 * Immutable — a new instance is created on each refresh.
 */
public final class AwsCredentials {
    private final String accessKeyId;
    private final String secretAccessKey;
    private final String sessionToken;
    private final long expiryTimestamp;

    /**
     * Creates a new AWS credentials holder with expiry.
     *
     * @param accessKeyId     AWS access key ID
     * @param secretAccessKey AWS secret access key
     * @param sessionToken    AWS session token, or null for long-lived credentials
     * @param expiryTimestamp epoch millis when these credentials expire, or 0 for never
     */
    public AwsCredentials(String accessKeyId, String secretAccessKey, String sessionToken, long expiryTimestamp) {
        this.accessKeyId = accessKeyId;
        this.secretAccessKey = secretAccessKey;
        this.sessionToken = sessionToken;
        this.expiryTimestamp = expiryTimestamp;
    }

    /**
     * Creates a new AWS credentials holder that never expires.
     *
     * @param accessKeyId     AWS access key ID
     * @param secretAccessKey AWS secret access key
     * @param sessionToken    AWS session token, or null for long-lived credentials
     */
    public AwsCredentials(String accessKeyId, String secretAccessKey, String sessionToken) {
        this(accessKeyId, secretAccessKey, sessionToken, 0);
    }

    /** Returns the AWS access key ID. */
    public String getAccessKeyId() {
        return accessKeyId;
    }

    /** Returns the AWS secret access key. */
    public String getSecretAccessKey() {
        return secretAccessKey;
    }

    /** Returns the AWS session token, or null if not present. */
    public String getSessionToken() {
        return sessionToken;
    }

    /** Returns the expiry timestamp in epoch millis, or 0 if never expires. */
    public long getExpiryTimestamp() {
        return expiryTimestamp;
    }

    /** Returns true if both access key and secret key are present and non-empty. */
    public boolean isComplete() {
        return accessKeyId != null && !accessKeyId.isEmpty() && secretAccessKey != null && !secretAccessKey.isEmpty();
    }

    /** Returns true if these credentials have an expiry and that expiry has passed. */
    public boolean isExpired() {
        return expiryTimestamp > 0 && System.currentTimeMillis() > expiryTimestamp;
    }
}
