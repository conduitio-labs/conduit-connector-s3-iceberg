package io.conduit;

import java.util.Map;
import java.util.Set;

import io.conduit.grpc.Specifier;
import io.conduit.grpc.Specifier.Specify.Request;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import software.amazon.awssdk.regions.Region;

import static java.util.stream.Collectors.toSet;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class SpecifierServiceTest {
    @Test
    void testSpecify() {
        var underTest = new SpecifierService();
        StreamObserver streamObserver = mock(StreamObserver.class);
        var captor = ArgumentCaptor.forClass(Specifier.Specify.Response.class);

        underTest.specify(
            Request.newBuilder().build(),
            streamObserver
        );

        verify(streamObserver).onNext(captor.capture());
        var response = captor.getValue();

        assertEquals("s3-iceberg", response.getName());
        assertTrue(response.getSourceParamsMap().isEmpty());

        var destParams = response.getDestinationParamsMap();
        assertEquals(8, destParams.size());

        assertParameterValidations(
            destParams,
            "catalog.catalog-impl",
            Set.of(
                "org.apache.iceberg.hadoop.HadoopCatalog",
                "org.apache.iceberg.jdbc.JdbcCatalog",
                "org.apache.iceberg.rest.RESTCatalog"
            )
        );

        assertParameterValidations(
            destParams,
            "s3.region",
            Region.regions().stream().map(Region::toString).collect(toSet())
        );
    }

    private void assertParameterValidations(Map<String, Specifier.Parameter> params,
                                            String name,
                                            Set<String> possibleValues) {
        assertTrue(params.containsKey(name));

        var validations = params.get(name).getValidationsList();
        assertEquals(2, validations.size());

        boolean requiredFound = false;
        boolean inclusionFound = false;
        for (var val : validations) {
            switch (val.getType()) {
                case TYPE_REQUIRED -> requiredFound = true;
                case TYPE_INCLUSION -> {
                    inclusionFound = true;
                    assertEquals(
                        possibleValues,
                        Set.of(
                            val.getValue().split(",")
                        )
                    );
                }
                default -> Assertions.fail("unexpected validation");
            }
        }

        assertTrue(requiredFound);
        assertTrue(inclusionFound);
    }

}