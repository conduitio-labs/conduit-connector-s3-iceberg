package io.conduit;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.UUID;

import com.google.protobuf.ByteString;
import io.conduit.grpc.Change;
import io.conduit.grpc.Data;
import io.conduit.grpc.Destination.Run.Request;
import io.conduit.grpc.Operation;
import io.conduit.grpc.Record;
import io.grpc.stub.StreamObserver;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class DefaultDestinationStreamIT {
    private SparkSession spark;
    private DestinationConfig config;

    @BeforeEach
    void setUp() {
        config = DestinationConfig.fromMap(Map.of("namespace", "webapp",
            "table.name", "logs",
            "catalog.name", "demo",
            "catalog.demo.catalog-impl", "org.apache.iceberg.rest.RESTCatalog",
            "catalog.demo.uri", "http://localhost:8181",
            "s3.endpoint", "http://localhost:9000",
            "s3.access-key-id", "admin",
            "s3.secret-access-key", "password"
        ));

        String catalogName = config.getCatalogName();

        var builder = SparkSession
            .builder()
            .master("local[*]")
            .appName("DefaultDestinationStreamIT")
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config("spark.sql.catalog." + catalogName, "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog." + catalogName + ".io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
            .config("spark.sql.catalog." + catalogName + ".s3.endpoint", config.getS3Endpoint())
            .config("spark.sql.catalog." + catalogName + ".s3.access-key-id", config.getS3AccessKeyId())
            .config("spark.sql.catalog." + catalogName + ".s3.secret-access-key", config.getS3SecretAccessKey())
            .config("spark.sql.defaultCatalog", catalogName);

        config.getCatalogProperties().forEach((k, v) -> builder.config("spark.sql." + k, v));
        spark = builder.getOrCreate();
    }

    @Test
    void testInsert() {
        String jsonString = """
                {
                "level": "debug",
                "event_time":  "%s",
                "message": "a debug message",
                "event_id": "%s",
                "integer_field": 123,
                "map_field": {"foo": "bar"}
                }
            """.formatted(LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME), UUID.randomUUID().toString());

        var observerMock = Mockito.mock(StreamObserver.class);
        DefaultDestinationStream stream = new DefaultDestinationStream(observerMock, spark, config.getCatalogName() + "." + config.getNamespace() + "." + config.getTableName());
        stream.onNext(
            Request.newBuilder()
                .setRecord(Record.newBuilder()
                    .setPayload(
                        Change.newBuilder()
                            .setAfter(
                                Data.newBuilder()
                                    .setRawData(ByteString.copyFromUtf8(jsonString))
                                    .build()
                            ).build()
                    ).setOperation(Operation.OPERATION_CREATE)
                    .build()
                ).build()
        );
    }
}