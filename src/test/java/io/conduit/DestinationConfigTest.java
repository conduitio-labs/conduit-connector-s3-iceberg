package io.conduit;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.regions.Region;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class DestinationConfigTest {
    @Test
    void testParse() {
        var input = Map.of(
            "catalog.name", "test_catalog",
            "namespace", "test_namespace",
            "table.name", "test_table",

            "catalog.catalog-impl", "org.apache.iceberg.rest.RESTCatalog",
            "catalog.uri", "http://localhost:8181",
            "s3.endpoint", "http://localhost:9000",
            "s3.accessKeyId", "test-access-key-id",
            "s3.secretAccessKey", "test-secret-access-key",
            "s3.region", Region.US_EAST_1.toString()
        );

        assertEquals(
            new DestinationConfig(
                "test_catalog",
                "test_namespace",
                "test_table",
                "http://localhost:9000",
                "test-access-key-id",
                "test-secret-access-key",
                Region.US_EAST_1.toString(),
                Map.of(
                    "catalog.catalog-impl", "org.apache.iceberg.rest.RESTCatalog",
                    "catalog.uri", "http://localhost:8181"
                )
            ),
            DestinationConfig.fromMap(input)
        );
    }

    @Test
    void testMissingRequired() {
        var required = List.of(
            "catalog.name", "catalog.catalog-impl",
            "namespace", "table.name",
            "s3.endpoint", "s3.accessKeyId", "s3.secretAccessKey"
        );
        var validCfg = Map.of(
            "catalog.name", "test_catalog",
            "catalog.catalog-impl", "org.apache.iceberg.rest.RESTCatalog",
            "catalog.uri", "http://localhost:8181",

            "namespace", "test_namespace",
            "table.name", "test_table",

            "s3.endpoint", "http://localhost:9000",
            "s3.accessKeyId", "test-access-key-id",
            "s3.secretAccessKey", "test-secret-access-key"
        );
        for (String property : required) {
            var input = new HashMap<>(validCfg);
            input.remove(property);

            IllegalArgumentException e = assertThrows(
                IllegalArgumentException.class,
                () -> DestinationConfig.fromMap(input),
                "expected IllegalArgumentException to be thrown for missing " + property
            );
            assertEquals(String.format("missing keys: [%s]", property), e.getMessage());
        }
    }
}
