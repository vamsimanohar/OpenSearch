/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.lakehouse;

import java.io.Closeable;

/**
 * Singleton holder for shared state across SPI-created instances.
 *
 * <p>OpenSearch SPI ({@code SPIClassIterator}) creates separate instances of
 * {@link LakehousePlugin} for each interface it implements. All instances
 * access shared state through this singleton.
 */
public final class LakehouseState implements Closeable {

    private static final LakehouseState INSTANCE = new LakehouseState();

    private LakehouseState() {}

    /** Returns the singleton instance. */
    public static LakehouseState instance() {
        return INSTANCE;
    }

    @Override
    public void close() {}
}
