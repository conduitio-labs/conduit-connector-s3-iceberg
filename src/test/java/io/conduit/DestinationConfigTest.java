package io.conduit;

import java.util.Map;

import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class DestinationConfigTest {
    @Test
    void testParse() {
        var input = Map.of(
            "catalog.impl", "org.apache.iceberg.rest.RESTCatalog",
            "catalog.uri", "http://localhost:8181",
            "catalog.s3.endpoint", "http://localhost:9000",
            "table.namespace", "webapp",
            "table.name", "dummy_type",

            "schema.string_field", "id:1;optional:true;type:string",
            "schema.timestamptz_field", "id:2;optional:false;type:timestamptz",
            "schema.list_field", "id:3;optional:false;type:list<string>",
            "schema.nested_list_field", "id:4;optional:true;type:list<list<string>>",
            "schema.map_field", "id:5;optional:true;type:map<string,boolean>"
        );

        DestinationConfig result = DestinationConfig.fromMap(input);
        assertEquals("org.apache.iceberg.rest.RESTCatalog", result.getCatalogImpl());
        assertEquals(
            Map.of(
                "catalog-impl", "org.apache.iceberg.rest.RESTCatalog",
                "uri", "http://localhost:8181",
                "s3.endpoint", "http://localhost:9000"
            ),
            result.getCatalogProperties()
        );
        assertEquals(
            TableIdentifier.of("webapp", "dummy_type"),
            result.getTableID()
        );
        var resultTypes = result.getSchema().columns().toArray();
        assertEquals(
            Types.NestedField.optional(1, "string_field", Types.StringType.get()),
            resultTypes[0]
        );
        assertEquals(
            Types.NestedField.required(2, "timestamp_field", Types.TimestampType.withZone()),
            resultTypes[1]
        );
        assertEquals(
            Types.NestedField.required(3, "list_field", Types.ListType.ofRequired(0, Types.StringType.get())),
            resultTypes[2]
        );
        assertEquals(
            Types.NestedField.required(4, "nested_list_field", Types.ListType.ofRequired(0, Types.StringType.get())),
            resultTypes[2]
        );
        assertEquals(
            Types.NestedField.required(5, "map_field", Types.MapType.ofRequired(0, 0, Types.StringType.get(), Types.BooleanType.get())),
            resultTypes[2]
        );
    }

}