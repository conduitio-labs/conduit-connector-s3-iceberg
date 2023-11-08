package io.conduit;

import java.util.Map;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class DestinationConfigTest {
    @Test
    void testParse() {
        var input = Map.of(
            "catalog.name", "test_catalog",
            "namespace", "test_namespace",
            "table.name", "test_table",

            "catalog.test_catalog.catalog-impl", "org.apache.iceberg.rest.RESTCatalog",
            "catalog.test_catalog.uri", "http://localhost:8181",
            "catalog.test_catalog.s3.endpoint", "http://localhost:9000"
        );

        Assertions.assertEquals(
            new DestinationConfig(
                "test_namespace",
                "test_table",
                "test_catalog",
                Map.of(
                    "catalog-impl", "org.apache.iceberg.rest.RESTCatalog",
                    "uri", "http://localhost:8181",
                    "s3.endpoint", "http://localhost:9000"
                )
            ),
            DestinationConfig.fromMap(input)
        );
    }


}