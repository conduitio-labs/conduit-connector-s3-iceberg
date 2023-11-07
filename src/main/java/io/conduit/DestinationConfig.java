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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import lombok.Getter;
import lombok.Setter;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

/**
 * Contains the configuration for a Conduit destination connector.
 */
@Getter
@Setter
public class DestinationConfig {
    // todo check if this always requires a bucket? auto-create?
    public static final String KEY_TABLE_NAMESPACE = "table.namespace";
    public static final String KEY_TABLE_NAME = "table.name";
    public static final String KEY_CATALOG_IMPL = "catalog.impl";
    public static final String CATALOG_PREFIX = "catalog.";
    public static final String SCHEMA_PREFIX = "schema.";

    private static final Map<String, Type.PrimitiveType> TYPES = ImmutableMap.<String, Type.PrimitiveType>builder()
        .put(Types.BooleanType.get().toString(), Types.BooleanType.get())
        .put(Types.IntegerType.get().toString(), Types.IntegerType.get())
        .put(Types.LongType.get().toString(), Types.LongType.get())
        .put(Types.FloatType.get().toString(), Types.FloatType.get())
        .put(Types.DoubleType.get().toString(), Types.DoubleType.get())
        .put(Types.DateType.get().toString(), Types.DateType.get())
        .put(Types.TimeType.get().toString(), Types.TimeType.get())
        .put(Types.TimestampType.withZone().toString(), Types.TimestampType.withZone())
        .put(Types.TimestampType.withoutZone().toString(), Types.TimestampType.withoutZone())
        .put(Types.StringType.get().toString(), Types.StringType.get())
        .put(Types.UUIDType.get().toString(), Types.UUIDType.get())
        .put(Types.BinaryType.get().toString(), Types.BinaryType.get())
        .buildOrThrow();

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
        Map<String, String> schemaMap = new HashMap<>();
        cfgMap.forEach((k, v) -> {
            if (k.startsWith(SCHEMA_PREFIX)) {
                schemaMap.put(k.replaceFirst(SCHEMA_PREFIX, ""), v);
            }
        });

        List<Types.NestedField> columns = new LinkedList<>();
        schemaMap.forEach((name, opts) -> {
            int id = 0;
            boolean optional = false;
            Type type = null;

            for (String optStr : opts.split(",")) {
                String[] opt = optStr.split(":");
                if (opt.length != 2) {
                    throw new IllegalArgumentException(
                        String.format("malformed schema spec: '%s'", optStr)
                    );
                }

                switch (opt[0]) {
                    case "id":
                        id = Integer.parseInt(opt[1]);
                        break;
                    case "optional":
                        optional = Boolean.parseBoolean(opt[1]);
                        break;
                    case "type":
                        type = TYPES.get(opt[1]);
                        break;
                    default:
                        throw new IllegalArgumentException("unknown field option: " + opt[0]);
                }
            }

            columns.add(
                Types.NestedField.of(id, optional, name, type)
            );
        });

        cfg.setSchema(new Schema(columns));
        return cfg;
    }
}
