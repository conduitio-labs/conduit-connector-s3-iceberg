package io.conduit;

import com.google.protobuf.ByteString;
import io.conduit.grpc.*;
import io.conduit.grpc.Record;
import io.grpc.stub.StreamObserver;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class DefaultDestinationStreamTest {

    private SparkSession spark;
    private DestinationConfig config;

    @Test
    void TestSparkSession() {
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
        stream.onNext(Destination.Run.Request.newBuilder()
                .setRecord(Record.newBuilder()
                        .setPayload(Change.newBuilder().setAfter(Data.newBuilder().setRawData(ByteString.copyFrom(jsonString, StandardCharsets.UTF_8)).build()).build())
                        .setOperation(Operation.OPERATION_CREATE)
                        .build())
                .build());
    }

    @BeforeEach
    private void setupSpark() {
        config = DestinationConfig.fromMap(Map.of("namespace", "webapp",
                "table.name", "logs",
                "catalog.name", "demo",
                "catalog.demo.catalog-impl", "org.apache.iceberg.rest.RESTCatalog",
                "catalog.demo.uri", "http://localhost:8181",
                "s3.endpoint", "http://localhost:9000",
                "s3.access-key-id", "admin",
                "s3.secret-access-key", "password"));
        String catalogName = config.getCatalogName();
        System.out.println("setting up spark builder");
        var builder = SparkSession
                .builder()
                .master("local[*]")
                .appName("Java API Demo")
                .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
                .config("spark.sql.catalog." + catalogName, "org.apache.iceberg.spark.SparkCatalog")
                .config("spark.sql.catalog." + catalogName + ".io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
                .config("spark.sql.catalog." + catalogName + ".s3.endpoint", config.getS3Endpoint())
                .config("spark.sql.catalog." + catalogName + ".s3.access-key-id", config.getS3AccessKeyId())
                .config("spark.sql.catalog." + catalogName + ".s3.secret-access-key", config.getS3SecretAccessKey())
                .config("spark.sql.defaultCatalog", catalogName)
                .config("spark.eventLog.enabled", "true")
                .config("spark.eventLog.dir", "/var/logs/spark-events")
                .config("spark.history.fs.logDirectory", "/var/logs/spark-events");
        System.out.println("adding catalog properties to builder");
        config.getCatalogProperties().forEach((k, v) -> builder.config("spark.sql." + k, v));
        System.out.println("get spark session");
        try {
            spark = builder.getOrCreate();
        } catch (Throwable e) {
            System.out.println("couldn't get spark session " + e.getMessage());
        }
        System.out.println("spark session: " + spark.conf().getAll());
    }
}