/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.lakehouse.catalog;

import software.amazon.awssdk.core.exception.SdkClientException;

import org.opensearch.test.OpenSearchTestCase;

import java.util.Map;

public class LakehouseCredentialsProviderTests extends OpenSearchTestCase {

    @Override
    public void tearDown() throws Exception {
        LakehouseCredentialsProvider.clear();
        super.tearDown();
    }

    public void testSetAndResolveBasicCredentials() {
        AwsCredentials creds = new AwsCredentials("AKID", "SECRET", null);
        LakehouseCredentialsProvider.set(creds);

        LakehouseCredentialsProvider provider = new LakehouseCredentialsProvider();
        software.amazon.awssdk.auth.credentials.AwsCredentials resolved = provider.resolveCredentials();

        assertEquals("AKID", resolved.accessKeyId());
        assertEquals("SECRET", resolved.secretAccessKey());
        assertFalse(resolved instanceof software.amazon.awssdk.auth.credentials.AwsSessionCredentials);
    }

    public void testSetAndResolveSessionCredentials() {
        AwsCredentials creds = new AwsCredentials("AKID", "SECRET", "TOKEN");
        LakehouseCredentialsProvider.set(creds);

        LakehouseCredentialsProvider provider = new LakehouseCredentialsProvider();
        software.amazon.awssdk.auth.credentials.AwsCredentials resolved = provider.resolveCredentials();

        assertTrue(resolved instanceof software.amazon.awssdk.auth.credentials.AwsSessionCredentials);
        software.amazon.awssdk.auth.credentials.AwsSessionCredentials session =
            (software.amazon.awssdk.auth.credentials.AwsSessionCredentials) resolved;
        assertEquals("AKID", session.accessKeyId());
        assertEquals("SECRET", session.secretAccessKey());
        assertEquals("TOKEN", session.sessionToken());
    }

    public void testResolveThrowsWhenNoCredentialsSet() {
        LakehouseCredentialsProvider provider = new LakehouseCredentialsProvider();
        expectThrows(SdkClientException.class, provider::resolveCredentials);
    }

    public void testResolveThrowsWhenIncompleteCredentials() {
        LakehouseCredentialsProvider.set(new AwsCredentials(null, "SECRET", null));
        LakehouseCredentialsProvider provider = new LakehouseCredentialsProvider();
        expectThrows(SdkClientException.class, provider::resolveCredentials);
    }

    public void testClearRemovesCredentials() {
        LakehouseCredentialsProvider.set(new AwsCredentials("AKID", "SECRET", null));
        LakehouseCredentialsProvider.clear();

        LakehouseCredentialsProvider provider = new LakehouseCredentialsProvider();
        expectThrows(SdkClientException.class, provider::resolveCredentials);
    }

    public void testGetReturnsSetCredentials() {
        AwsCredentials creds = new AwsCredentials("AKID", "SECRET", null);
        LakehouseCredentialsProvider.set(creds);
        assertSame(creds, LakehouseCredentialsProvider.get());
    }

    public void testGetReturnsNullWhenNotSet() {
        assertNull(LakehouseCredentialsProvider.get());
    }

    public void testCreateFactoryWithProperties() {
        LakehouseCredentialsProvider provider = LakehouseCredentialsProvider.create(Map.of("key", "val"));
        assertNotNull(provider);
    }

    public void testCreateFactoryNoArgs() {
        LakehouseCredentialsProvider provider = LakehouseCredentialsProvider.create();
        assertNotNull(provider);
    }
}
