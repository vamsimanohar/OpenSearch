/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.lakehouse.catalog;

import org.opensearch.test.OpenSearchTestCase;

public class AwsCredentialsTests extends OpenSearchTestCase {

    public void testIsComplete() {
        AwsCredentials creds = new AwsCredentials("AKID", "SECRET", null);
        assertTrue(creds.isComplete());
    }

    public void testIsCompleteWithSession() {
        AwsCredentials creds = new AwsCredentials("AKID", "SECRET", "TOKEN");
        assertTrue(creds.isComplete());
    }

    public void testIsNotCompleteNullAccessKey() {
        AwsCredentials creds = new AwsCredentials(null, "SECRET", null);
        assertFalse(creds.isComplete());
    }

    public void testIsNotCompleteEmptyAccessKey() {
        AwsCredentials creds = new AwsCredentials("", "SECRET", null);
        assertFalse(creds.isComplete());
    }

    public void testIsNotCompleteNullSecretKey() {
        AwsCredentials creds = new AwsCredentials("AKID", null, null);
        assertFalse(creds.isComplete());
    }

    public void testIsNotCompleteEmptySecretKey() {
        AwsCredentials creds = new AwsCredentials("AKID", "", null);
        assertFalse(creds.isComplete());
    }

    public void testNeverExpiresDefault() {
        AwsCredentials creds = new AwsCredentials("AKID", "SECRET", null);
        assertFalse(creds.isExpired());
        assertEquals(0, creds.getExpiryTimestamp());
    }

    public void testNotExpiredFutureTimestamp() {
        long futureMs = System.currentTimeMillis() + 3_600_000;
        AwsCredentials creds = new AwsCredentials("AKID", "SECRET", "TOKEN", futureMs);
        assertFalse(creds.isExpired());
    }

    public void testExpiredPastTimestamp() {
        long pastMs = System.currentTimeMillis() - 1_000;
        AwsCredentials creds = new AwsCredentials("AKID", "SECRET", "TOKEN", pastMs);
        assertTrue(creds.isExpired());
    }

    public void testGetters() {
        AwsCredentials creds = new AwsCredentials("AKID", "SECRET", "TOKEN", 12345L);
        assertEquals("AKID", creds.getAccessKeyId());
        assertEquals("SECRET", creds.getSecretAccessKey());
        assertEquals("TOKEN", creds.getSessionToken());
        assertEquals(12345L, creds.getExpiryTimestamp());
    }
}
