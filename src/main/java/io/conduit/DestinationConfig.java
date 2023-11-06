/*
 * Copyright 2022 Meroxa, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.conduit;

import java.util.HashMap;
import java.util.Map;

import lombok.Getter;
import lombok.Setter;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;

/**
 * Contains the configuration for a Conduit destination connector.
 */
@Getter
@Setter
public class DestinationConfig {
    public static final String KEY_TABLE_NAMESPACE = "table.namespace";
    public static final String KEY_TABLE_NAME = "table.name";
    public static final String KEY_CATALOG_IMPL = "catalog.impl";
    public static final String CATALOG_PREFIX = "catalog.";

    private String catalogImpl;
    private Map<String, String> catalogProperties;
    private TableIdentifier tableID;
    private Schema schema;

    /**
     * Creates a new <code>DestinationConfig</code> instance from a map with configuration parameters.
     */
    public static DestinationConfig fromMap(Map<String, String> cfgMap) {
        DestinationConfig cfg = new DestinationConfig();
        if (Utils.isEmpty(cfgMap)) {
            return cfg;
        }

        // Catalog implementation class
        String catalogImpl = cfgMap.get(KEY_CATALOG_IMPL);
        if (catalogImpl == null) {
            throw new IllegalArgumentException("missing " + KEY_CATALOG_IMPL);
        }
        cfg.setCatalogImpl(catalogImpl);

        // Catalog properties
        Map<String, String> catalogProps = new HashMap<>();
        cfgMap.forEach((key, value) -> {
            if (key.startsWith(CATALOG_PREFIX)) {
                catalogProps.put(key.replaceFirst(CATALOG_PREFIX, ""), value);
            }
        });
        catalogProps.remove(KEY_CATALOG_IMPL.replaceFirst(CATALOG_PREFIX, ""));
        catalogProps.put(CatalogProperties.CATALOG_IMPL, catalogImpl);
        cfg.setCatalogProperties(catalogProps);

        // Table ID
        cfg.setTableID(
            TableIdentifier.of(
                Namespace.of(cfgMap.get(KEY_TABLE_NAMESPACE)),
                cfgMap.get(KEY_TABLE_NAME)
            )
        );

        // Schema
        return cfg;
    }
}
