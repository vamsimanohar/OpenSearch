/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.lakehouse.cluster;

import org.opensearch.Version;
import org.opensearch.cluster.AbstractNamedDiffable;
import org.opensearch.cluster.NamedDiff;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Custom cluster state metadata for storing lakehouse catalog configs and table bindings.
 */
public class LakehouseMetadata extends AbstractNamedDiffable<Metadata.Custom> implements Metadata.Custom {

    public static final String TYPE = "lakehouse";
    public static final LakehouseMetadata EMPTY = new LakehouseMetadata(Collections.emptyMap(), Collections.emptyMap());

    private final Map<String, Map<String, String>> catalogs;  // name -> config map
    private final Map<String, Map<String, String>> tables;    // name -> binding map

    public LakehouseMetadata(Map<String, Map<String, String>> catalogs, Map<String, Map<String, String>> tables) {
        this.catalogs = Collections.unmodifiableMap(catalogs);
        this.tables = Collections.unmodifiableMap(tables);
    }

    public LakehouseMetadata(StreamInput in) throws IOException {
        this.catalogs = in.readMap(
            StreamInput::readString,
            i -> i.readMap(StreamInput::readString, StreamInput::readString)
        );
        this.tables = in.readMap(
            StreamInput::readString,
            i -> i.readMap(StreamInput::readString, StreamInput::readString)
        );
    }

    public Map<String, Map<String, String>> catalogs() {
        return catalogs;
    }

    public Map<String, Map<String, String>> tables() {
        return tables;
    }

    @Override
    public String getWriteableName() {
        return TYPE;
    }

    @Override
    public Version getMinimalSupportedVersion() {
        return Version.V_3_0_0;
    }

    @Override
    public EnumSet<Metadata.XContentContext> context() {
        return Metadata.API_AND_GATEWAY;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(catalogs, StreamOutput::writeString, (o, m) -> o.writeMap(m, StreamOutput::writeString, StreamOutput::writeString));
        out.writeMap(tables, StreamOutput::writeString, (o, m) -> o.writeMap(m, StreamOutput::writeString, StreamOutput::writeString));
    }

    public static NamedDiff<Metadata.Custom> readDiffFrom(StreamInput in) throws IOException {
        return readDiffFrom(Metadata.Custom.class, TYPE, in);
    }

    public static LakehouseMetadata fromXContent(XContentParser parser) throws IOException {
        Map<String, Map<String, String>> catalogs = new HashMap<>();
        Map<String, Map<String, String>> tables = new HashMap<>();

        XContentParser.Token token;
        String currentFieldName = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                if ("catalogs".equals(currentFieldName)) {
                    catalogs = parseStringMapOfMaps(parser);
                } else if ("tables".equals(currentFieldName)) {
                    tables = parseStringMapOfMaps(parser);
                } else {
                    parser.skipChildren();
                }
            }
        }
        return new LakehouseMetadata(catalogs, tables);
    }

    private static Map<String, Map<String, String>> parseStringMapOfMaps(XContentParser parser) throws IOException {
        Map<String, Map<String, String>> result = new HashMap<>();
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                String name = parser.currentName();
                parser.nextToken(); // START_OBJECT
                Map<String, String> inner = new HashMap<>();
                while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                    String key = parser.currentName();
                    parser.nextToken();
                    inner.put(key, parser.text());
                }
                result.put(name, inner);
            }
        }
        return result;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("catalogs");
        for (Map.Entry<String, Map<String, String>> entry : catalogs.entrySet()) {
            builder.startObject(entry.getKey());
            for (Map.Entry<String, String> inner : entry.getValue().entrySet()) {
                builder.field(inner.getKey(), inner.getValue());
            }
            builder.endObject();
        }
        builder.endObject();

        builder.startObject("tables");
        for (Map.Entry<String, Map<String, String>> entry : tables.entrySet()) {
            builder.startObject(entry.getKey());
            for (Map.Entry<String, String> inner : entry.getValue().entrySet()) {
                builder.field(inner.getKey(), inner.getValue());
            }
            builder.endObject();
        }
        builder.endObject();

        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LakehouseMetadata that = (LakehouseMetadata) o;
        return Objects.equals(catalogs, that.catalogs) && Objects.equals(tables, that.tables);
    }

    @Override
    public int hashCode() {
        return Objects.hash(catalogs, tables);
    }
}
