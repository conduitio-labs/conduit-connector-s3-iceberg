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

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.function.Predicate.not;

/**
 * Contains the configuration for a Conduit destination connector.
 */
@NoArgsConstructor
@AllArgsConstructor
@Data
public class DestinationConfig {
    public static final Logger logger = LoggerFactory.getLogger(DestinationConfig.class);
    private static final ObjectMapper mapper = new ObjectMapper();
    private static final String CATALOG_PREFIX = "catalog.";
    private static final List<String> REQUIRED_KEYS = List.of(
        "catalog.name", "catalog.catalog-impl",
        "namespace", "table.name",
        "s3.endpoint", "s3.access-key-id", "s3.secret-access-key"
    );

    @JsonProperty(value = "catalog.name")
    private String catalogName;

    @JsonProperty(value = "namespace")
    private String namespace;

    @JsonProperty(value = "table.name")
    private String tableName;

    @JsonProperty(value = "s3.endpoint")
    private String s3Endpoint;

    @JsonProperty(value = "s3.access-key-id")
    private String s3AccessKeyId;

    @JsonProperty(value = "s3.secret-access-key")
    private String s3SecretAccessKey;

    private Map<String, String> catalogProperties = new HashMap<>();

    /**
     * Creates a new <code>DestinationConfig</code> instance from a map with configuration parameters.
     * The map is required to have:
     * <li><code>namespace</code></li>
     * <li><code>table.name</code></li>
     * <li><code>catalog.name</code></li>
     * <li><code>s3.endpoint</code></li>
     * <li><code>s3.endpoint</code></li>
     * <li><code>s3.access-key-id</code></li>
     * <li><code>s3.secret-access-key</code></li>
     * <p>
     * The catalog properties need to be prefixed with <code>catalog.</code>.
     * If, for example, the catalog has a parameter <code>uri=https://example.com</code>,
     * then the following should be added to the map:
     * <code>catalog.uri=https://example.com</code>
     *
     * @throws IllegalArgumentException if required keys are missing or if there are unknown parameters.
     */
    public static DestinationConfig fromMap(Map<String, String> cfgMap) {
        checkRequired(cfgMap);

        DestinationConfig cfg = mapper.convertValue(cfgMap, DestinationConfig.class);

        var unknownProps = cfg.getCatalogProperties().keySet().stream()
            .filter(not(k -> k.startsWith(CATALOG_PREFIX)))
            .toList();
        if (!Utils.isEmpty(unknownProps)) {
            throw new IllegalArgumentException("unknown properties: " + unknownProps);
        }

        return cfg;
    }

    private static void checkRequired(Map<String, String> cfgMap) {
        List<String> missing = REQUIRED_KEYS.stream()
            .filter(not(cfgMap::containsKey))
            .toList();

        if (!Utils.isEmpty(missing)) {
            throw new IllegalArgumentException("missing keys: " + missing);
        }
    }

    @JsonAnyGetter
    public Map<String, String> otherFields() {
        return catalogProperties;
    }

    @JsonAnySetter
    public void setOtherField(String name, String value) {
        catalogProperties.put(name, value);
    }

    public String fullTableName() {
        return getCatalogName() + "." + getNamespace() + "." + getTableName();
    }
}
