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
import java.util.List;
import java.util.Map;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.function.Predicate.not;

/**
 * Contains the configuration for a Conduit destination connector.
 */
@Getter
@Setter
@AllArgsConstructor
@EqualsAndHashCode
@ToString
public class DestinationConfig {
    public static final Logger logger = LoggerFactory.getLogger(DestinationConfig.class);

    public static final String KEY_NAMESPACE = "namespace";
    public static final String KEY_TABLE_NAME = "table.name";
    public static final String KEY_CATALOG_NAME = "catalog.name";
    private static final List<String> REQUIRED_KEYS = List.of(KEY_NAMESPACE, KEY_TABLE_NAME, KEY_CATALOG_NAME);

    private static final String CATALOG_PREFIX = "catalog.";

    private final String namespace;
    private final String tableName;
    private final String catalogName;
    private final Map<String, String> catalogProperties;

    /**
     * Creates a new <code>DestinationConfig</code> instance from a map with configuration parameters.
     * The map is required to have:
     * <li><code>namespace</code></li>
     * <li><code>table.name</code></li>
     * <li><code>catalog.name</code></li>
     *
     * The catalog properties need to prefixed with <code>catalog.catalog_name</code>.
     * If, for example, the catalog's name is <code>ProdCatalog</code>, and it has a
     * parameter <code>uri=https://example.com</code>, then the following should be added to the map:
     * <code>catalog.ProdCatalog.uri=https://example.com</code>
     *
     * @throws IllegalArgumentException if required keys are missing or if there are unknown parameters.
     */
    public static DestinationConfig fromMap(Map<String, String> cfgMap) {
        checkRequired(cfgMap);

        String namespace = cfgMap.get(KEY_NAMESPACE);
        String tableName = cfgMap.get(KEY_TABLE_NAME);
        String catalogName = cfgMap.get(KEY_CATALOG_NAME);

        // Catalog properties, prefixed with catalog.catalog_name
        var catalogPropsPrefixed = new HashMap<>(cfgMap);
        catalogPropsPrefixed.remove(KEY_CATALOG_NAME);
        catalogPropsPrefixed.remove(KEY_NAMESPACE);
        catalogPropsPrefixed.remove(KEY_TABLE_NAME);

        var unknownProps = catalogPropsPrefixed.keySet().stream()
            .filter(not(k -> k.startsWith(CATALOG_PREFIX + catalogName + ".")))
            .toList();
        if (!Utils.isEmpty(unknownProps)) {
            throw new IllegalArgumentException("unknown properties: " + unknownProps);
        }

        Map<String, String> catalogProperties = new HashMap<>();
        for (Map.Entry<String, String> entry : catalogPropsPrefixed.entrySet()) {
            catalogProperties.put(
                entry.getKey().replaceFirst(CATALOG_PREFIX + catalogName + ".", ""),
                entry.getValue()
            );
        }

        return new DestinationConfig(
            namespace,
            tableName,
            catalogName,
            catalogProperties
        );
    }

    private static void checkRequired(Map<String, String> cfgMap) {
        List<String> missing = REQUIRED_KEYS.stream()
            .filter(not(cfgMap::containsKey))
            .toList();

        if (!Utils.isEmpty(missing)) {
            throw new IllegalArgumentException("missing keys: " + missing);
        }

        String catalogImplKey = CATALOG_PREFIX + cfgMap.get(KEY_CATALOG_NAME) + ".catalog-impl";
        if (!cfgMap.containsKey(catalogImplKey)) {
            throw new IllegalArgumentException("missing " + catalogImplKey);
        }
    }

}
