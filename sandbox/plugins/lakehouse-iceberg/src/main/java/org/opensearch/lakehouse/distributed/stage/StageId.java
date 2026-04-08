/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.lakehouse.distributed.stage;

import java.util.Objects;

/** Unique identifier for a stage in the distributed execution plan. */
public final class StageId {
    private final int id;

    /**
     * Creates a new StageId with the given numeric identifier.
     *
     * @param id the numeric stage identifier
     */
    public StageId(int id) { this.id = id; }
    /** Returns the numeric identifier. */
    public int getId() { return id; }

    @Override
    public boolean equals(Object o) {
        return o instanceof StageId s && s.id == this.id;
    }

    @Override
    public int hashCode() { return Integer.hashCode(id); }

    @Override
    public String toString() { return "Stage-" + id; }
}
