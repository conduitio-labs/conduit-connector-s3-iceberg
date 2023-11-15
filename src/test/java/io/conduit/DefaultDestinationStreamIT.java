package io.conduit;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.UUID;

import com.google.protobuf.ByteString;
import io.conduit.grpc.Change;
import io.conduit.grpc.Data;
import io.conduit.grpc.Destination;
import io.conduit.grpc.Destination.Run.Request;
import io.conduit.grpc.Operation;
import io.conduit.grpc.Record;
import io.grpc.stub.StreamObserver;
import lombok.SneakyThrows;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.Schema;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

class DefaultDestinationStreamIT {
    SparkSession spark;
    DestinationConfig config;
    Map<String, String> catalogProps;
    Schema schema = new Schema(
        Types.NestedField.required(1, "level", Types.StringType.get()),
        Types.NestedField.required(2, "event_time", Types.TimestampType.withZone()),
        Types.NestedField.required(3, "message", Types.StringType.get()),
        Types.NestedField.optional(4, "call_stack", Types.ListType.ofRequired(5, Types.StringType.get())),
        Types.NestedField.required(6, "event_id", Types.StringType.get()),
        Types.NestedField.optional(7, "integer_field", Types.IntegerType.get()),
        Types.NestedField.optional(8, "map_field", Types.MapType.ofOptional(123, 456, Types.StringType.get(), Types.StringType.get()))
    );

    Namespace namespace = Namespace.of("webapp");
    TableIdentifier tableId = TableIdentifier.of(namespace, "logs");

    @BeforeEach
    @SneakyThrows
    void setUp() {
        config = DestinationConfig.fromMap(Map.of(
            "catalog.name", "demo",
            "namespace", "webapp",
            "table.name", "logs",
            "catalog.catalog-impl", "org.apache.iceberg.rest.RESTCatalog",
            "catalog.uri", "http://localhost:8181",
            "s3.endpoint", "http://localhost:9000",
            "s3.access-key-id", "admin",
            "s3.secret-access-key", "password"
        ));
        catalogProps = Map.of(
            CatalogProperties.CATALOG_IMPL, "org.apache.iceberg.rest.RESTCatalog",
            CatalogProperties.URI, "http://localhost:8181",
            CatalogProperties.WAREHOUSE_LOCATION, "s3a://warehouse/wh",
            CatalogProperties.FILE_IO_IMPL, "org.apache.iceberg.aws.s3.S3FileIO",
            S3FileIOProperties.ENDPOINT, config.getS3Endpoint(),
            S3FileIOProperties.ACCESS_KEY_ID, config.getS3AccessKeyId(),
            S3FileIOProperties.SECRET_ACCESS_KEY, config.getS3SecretAccessKey()
        );

        spark = initSpark();
        initTable();
    }

    @SneakyThrows
    private void initTable() {
        try (RESTCatalog catalog = new RESTCatalog()) {
            Configuration conf = new Configuration();
            catalog.setConf(conf);
            catalog.initialize("demo", catalogProps);

            if (!catalog.namespaceExists(namespace)) {
                catalog.createNamespace(namespace);
            }
            if (!catalog.tableExists(tableId)) {
                catalog.createTable(tableId, schema);
            }
        }
    }

    private SparkSession initSpark() {
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

        return builder.getOrCreate();
    }

    @Test
    @SneakyThrows
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

        var observerMock = mock(StreamObserver.class);
        DefaultDestinationStream underTest = new DefaultDestinationStream(
            observerMock,
            spark,
            config.getCatalogName() + "." + config.getNamespace() + "." + config.getTableName()
        );
        underTest.onNext(
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
        var captor = ArgumentCaptor.forClass(Destination.Run.Response.class);
        verify(observerMock).onNext(captor.capture());
        verify(observerMock, never()).onError(any());
    }
}
