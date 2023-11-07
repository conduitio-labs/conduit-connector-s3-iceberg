package io.conduit;

import java.util.Map;

import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

class DestinationConfigTest {
    @Test
    void testParse() {
        var input = Map.of(
            "catalog.impl", "org.apache.iceberg.rest.RESTCatalog",
            "catalog.uri", "http://localhost:8181",
            "catalog.s3.endpoint", "http://localhost:9000",
            "table.namespace", "webapp",
            "table.name", "logs",

            "schema.level", "id:1,optional:true,type:string",
            "schema.event_time", "id:2,optional:false,type:timestamptz"
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
            TableIdentifier.of("webapp", "logs"),
            result.getTableID()
        );
        assertArrayEquals(
           new Types.NestedField[]{
               Types.NestedField.optional(1, "level", Types.StringType.get()),
               Types.NestedField.required(2, "event_time", Types.TimestampType.withZone())
           },
            result.getSchema().columns().toArray()
        );
    }

}