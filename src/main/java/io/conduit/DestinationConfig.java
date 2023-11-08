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
import java.util.Objects;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.collect.ImmutableMap;
import lombok.Getter;
import lombok.Setter;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types.*;

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

    public static final Pattern listTypeRegex = Pattern.compile("^list<(.+)>$");
    public static final Pattern mapTypeRegex = Pattern.compile("^map<(.+)\\s*,\\s*(.+)>$");

    // Maps primitive type names to types
    private static final Map<String, Type.PrimitiveType> PRIMITIVE_TYPES =
        ImmutableMap.<String, Type.PrimitiveType>builder()
            .put(BooleanType.get().toString(), BooleanType.get())
            .put(IntegerType.get().toString(), IntegerType.get())
            .put(LongType.get().toString(), LongType.get())
            .put(FloatType.get().toString(), FloatType.get())
            .put(DoubleType.get().toString(), DoubleType.get())
            .put(DateType.get().toString(), DateType.get())
            .put(TimeType.get().toString(), TimeType.get())
            .put(TimestampType.withZone().toString(), TimestampType.withZone())
            .put(TimestampType.withoutZone().toString(), TimestampType.withoutZone())
            .put(StringType.get().toString(), StringType.get())
            .put(UUIDType.get().toString(), UUIDType.get())
            .put(BinaryType.get().toString(), BinaryType.get())
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

        List<NestedField> columns = new LinkedList<>();
        schemaMap.forEach((name, opts) -> {
            int id = 0;
            boolean optional = false;
            Type type = null;

            for (String optStr : opts.split(";")) {
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
                        type = getType(opt[1]);
                        break;
                    default:
                        throw new IllegalArgumentException("unknown field option: " + opt[0]);
                }
            }

            Objects.requireNonNull(type, "type not found or not provided");
            columns.add(
                NestedField.of(id, optional, name, type)
            );
        });

        cfg.setSchema(new Schema(columns));
        return cfg;
    }

    private static Type getType(String typeName) {
        Type.PrimitiveType type = PRIMITIVE_TYPES.get(typeName);
        if (type != null) {
            return type;
        }
        // not in primitive types, could be a nested
        // Check if input1 matches the pattern and extract "something"
        Matcher matcher = listTypeRegex.matcher(typeName);
        if (matcher.matches()) {
            return ListType.ofRequired(Math.abs(new Random().nextInt()), getType(matcher.group(1)));
        }

        matcher = mapTypeRegex.matcher(typeName);
        if (matcher.matches()) {
            return MapType.ofRequired(Math.abs(new Random().nextInt()), Math.abs(new Random().nextInt()), getType(matcher.group(1)), getType(matcher.group(2)));
        }

        return null;
    }
}
