package io.conduit;

import java.util.Map;

import org.apache.iceberg.Schema;
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
            "table.name", "logs",
            "schema.level", "1,required,string",
            "schema.event_time", "2,required,timestamptz"
        );

        DestinationConfig result = DestinationConfig.fromMap(input);
        assertEquals("org.apache.iceberg.rest.RESTCatalog", result.getCatalogImpl());
        assertEquals(
            Map.of(
                "uri", "http://localhost:8181",
                "s3.endpoint", "http://localhost:9000"
            ),
            result.getCatalogProperties()
        );
        assertEquals(
            TableIdentifier.of("webapp", "logs"),
            result.getTableID());
        assertEquals(
            new Schema(
                Types.NestedField.required(1, "level", Types.StringType.get()),
                Types.NestedField.required(2, "event_time", Types.TimestampType.withZone())
            ),
            result.getSchema());
    }

}