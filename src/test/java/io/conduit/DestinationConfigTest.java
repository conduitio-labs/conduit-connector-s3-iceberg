package io.conduit;

import java.util.Map;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class DestinationConfigTest {
    @Test
    void testParse() {
        var input = Map.of(
            "catalog.name", "test_catalog",
            "namespace", "test_namespace",
            "table.name", "test_table",

            "catalog.test_catalog.catalog-impl", "org.apache.iceberg.rest.RESTCatalog",
            "catalog.test_catalog.uri", "http://localhost:8181",
            "s3.endpoint", "http://localhost:9000",
            "s3.access-key-id", "test-access-key-id",
            "s3.secret-access-key", "test-secret-access-key"
        );

        assertEquals(
            new DestinationConfig(
                "test_catalog",
                "test_namespace",
                "test_table",
                "http://localhost:9000",
                "test-access-key-id",
                "test-secret-access-key",
                Map.of(
                    "catalog.test_catalog.catalog-impl", "org.apache.iceberg.rest.RESTCatalog",
                    "catalog.test_catalog.uri", "http://localhost:8181"
                )
            ),
            DestinationConfig.fromMap(input)
        );
    }

    @Test
    void testMissingCatalogName() {
        var input = Map.of(
            "namespace", "test_namespace",
            "table.name", "test_table",

            "catalog.test_catalog.catalog-impl", "org.apache.iceberg.rest.RESTCatalog",
            "catalog.test_catalog.uri", "http://localhost:8181",
            "s3.endpoint", "http://localhost:9000",
            "s3.access-key-id", "test-access-key-id",
            "s3.secret-access-key", "test-secret-access-key"
        );

        IllegalArgumentException e = assertThrows(
            IllegalArgumentException.class,
            () -> DestinationConfig.fromMap(input)
        );
        assertEquals("missing keys: [catalog.name]", e.getMessage());
    }
}
