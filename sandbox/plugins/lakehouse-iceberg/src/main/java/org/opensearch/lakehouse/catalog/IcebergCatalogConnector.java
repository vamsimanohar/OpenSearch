/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.lakehouse.catalog;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;

import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Manages Iceberg catalog connections. Phase 1 supports Glue only.
 */
public class IcebergCatalogConnector {

    private static final String S3_PREFIX = "s3://";
    private static final String FILE_PREFIX = "file://";

    private final ConcurrentHashMap<String, Catalog> catalogs = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, CatalogConfig> configs = new ConcurrentHashMap<>();

    /** Creates a new catalog connector instance. */
    public IcebergCatalogConnector() {}

    /**
     * Registers a new Iceberg catalog connection.
     *
     * @param name   logical name for the catalog
     * @param config catalog configuration
     * @throws IllegalArgumentException          if the name is already registered or the warehouse URI is invalid
     * @throws UnsupportedOperationException     if the catalog type is not yet supported
     */
    public void registerCatalog(String name, CatalogConfig config) {
        if (catalogs.containsKey(name)) {
            throw new IllegalArgumentException("Catalog already registered: " + name);
        }

        validateWarehouseUri(config.warehouse());
        validateCatalogType(config.catalogType());

        Map<String, String> properties = buildCatalogProperties(config);

        // CatalogUtil.buildIcebergCatalog uses DynConstructors which relies on
        // Thread.contextClassLoader. When called from an SPI-created extension instance,
        // the context classloader may not include Iceberg/Hadoop classes.
        ClassLoader prev = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(getClass().getClassLoader());
            Catalog catalog = CatalogUtil.buildIcebergCatalog(name, properties, new Configuration());
            catalogs.put(name, catalog);
            configs.put(name, config);
        } finally {
            Thread.currentThread().setContextClassLoader(prev);
        }
    }

    /**
     * Loads an Iceberg table from a registered catalog.
     *
     * @param catalogName name of the registered catalog
     * @param tableId     Iceberg table identifier
     * @return the loaded Iceberg {@link Table}
     * @throws IllegalArgumentException if the catalog is not registered
     */
    public Table loadTable(String catalogName, TableIdentifier tableId) {
        Catalog catalog = catalogs.get(catalogName);
        if (catalog == null) {
            throw new IllegalArgumentException("Catalog not registered: " + catalogName);
        }
        return catalog.loadTable(tableId);
    }

    /**
     * Returns the set of registered catalog names.
     */
    public Set<String> listCatalogs() {
        return Collections.unmodifiableSet(catalogs.keySet());
    }

    /**
     * Validates the warehouse URI. S3 URIs are required for production catalogs.
     * File URIs are allowed for HADOOP catalogs (local testing).
     */
    static void validateWarehouseUri(String warehouse) {
        if (warehouse == null) {
            throw new IllegalArgumentException("Warehouse URI must not be null");
        }
        String lower = warehouse.toLowerCase(Locale.ROOT);
        if (!lower.startsWith(S3_PREFIX) && !lower.startsWith(FILE_PREFIX)) {
            throw new IllegalArgumentException(
                "Warehouse URI must start with 's3://' or 'file://'. Received: " + warehouse
            );
        }
    }

    /**
     * Validates the catalog type. Phase 1 supports Glue, REST, and Hadoop.
     */
    static void validateCatalogType(CatalogType type) {
        if (type == CatalogType.HIVE) {
            throw new UnsupportedOperationException("HIVE catalog type is not supported in Phase 1");
        }
    }

    private Map<String, String> buildCatalogProperties(CatalogConfig config) {
        Map<String, String> props = new HashMap<>();
        props.put(CatalogProperties.WAREHOUSE_LOCATION, config.warehouse());
        props.put(CatalogUtil.ICEBERG_CATALOG_TYPE, catalogTypeToIceberg(config.catalogType()));

        if (config.uri() != null) {
            props.put(CatalogProperties.URI, config.uri());
        }
        return props;
    }

    private String catalogTypeToIceberg(CatalogType type) {
        switch (type) {
            case GLUE:
                return CatalogUtil.ICEBERG_CATALOG_TYPE_GLUE;
            case HADOOP:
                return CatalogUtil.ICEBERG_CATALOG_TYPE_HADOOP;
            case REST:
                return CatalogUtil.ICEBERG_CATALOG_TYPE_REST;
            default:
                throw new UnsupportedOperationException("Unsupported catalog type: " + type);
        }
    }
}
