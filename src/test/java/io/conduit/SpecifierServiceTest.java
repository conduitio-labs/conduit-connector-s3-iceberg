package io.conduit;

import java.util.Set;

import io.conduit.grpc.Specifier;
import io.conduit.grpc.Specifier.Specify.Request;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

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
        assertEquals(7, destParams.size());
        assertTrue(destParams.containsKey("catalog.catalog-impl"));
        var validations = destParams.get("catalog.catalog-impl").getValidationsList();
        assertEquals(2, validations.size());

        boolean requiredFound = false;
        boolean inclusionFound = false;
        for (var val : validations) {
            switch (val.getType()) {
                case TYPE_REQUIRED ->
                    requiredFound = true;
                case TYPE_INCLUSION -> {
                    inclusionFound = true;
                    assertEquals(
                        Set.of(
                            "org.apache.iceberg.hadoop.HadoopCatalog",
                            "org.apache.iceberg.jdbc.JdbcCatalog",
                            "org.apache.iceberg.rest.RESTCatalog"
                        ),
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