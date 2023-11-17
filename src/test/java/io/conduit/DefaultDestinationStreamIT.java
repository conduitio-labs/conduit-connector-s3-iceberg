package io.conduit;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.google.protobuf.ByteString;
import io.conduit.grpc.Change;
import io.conduit.grpc.Data;
import io.conduit.grpc.Destination.Run.Request;
import io.conduit.grpc.Operation;
import io.conduit.grpc.Record;
import io.grpc.stub.StreamObserver;
import lombok.SneakyThrows;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.SparkSession;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.regions.Region;

import static org.junit.jupiter.api.Assertions.assertEquals;
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
    TableIdentifier tableId = TableIdentifier.of(namespace, "DefaultDestinationStreamIT");

    @BeforeEach
    @SneakyThrows
    void setUp() {
        config = DestinationConfig.fromMap(Map.of(
            "catalog.name", "demo",
            "namespace", namespace.toString(),
            "table.name", tableId.name(),
            "catalog.catalog-impl", "org.apache.iceberg.rest.RESTCatalog",
            "catalog.uri", "http://localhost:8181",
            "s3.endpoint", "http://localhost:9000",
            "s3.access-key-id", "admin",
            "s3.secret-access-key", "password",
            "s3.region", Region.US_EAST_1.toString()
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

        spark = SparkUtils.create(DefaultDestinationStreamIT.class.getName(), config);

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
            if (catalog.tableExists(tableId)) {
                catalog.dropTable(tableId);
            }
            catalog.createTable(tableId, schema);
        }
    }

    private SparkSession initSpark() {
        String catalogName = config.getCatalogName();
        System.setProperty("aws.region", config.getS3Region());
        String prefix = "spark.sql.catalog." + catalogName;
        var builder = SparkSession
            .builder()
            .master("local[*]")
            .appName("Java API Demo")
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config(prefix, "org.apache.iceberg.spark.SparkCatalog")
            .config(prefix + ".io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
            .config(prefix + ".s3.endpoint", config.getS3Endpoint())
            .config(prefix + ".s3.access-key-id", config.getS3AccessKeyId())
            .config(prefix + ".s3.secret-access-key", config.getS3SecretAccessKey())
            .config("spark.sql.defaultCatalog", catalogName)
            .config("spark.driver.extraJavaOptions", "-Daws.region=us-east-1")
            .config("spark.executor.extraJavaOptions", "-Daws.region=us-east-1");

        config.getCatalogProperties().forEach((k, v) -> {
            // keys are in the form of catalog.propertyName
            builder.config(prefix + "." + k.replaceFirst("catalog.", ""), v);
        });

        return builder.getOrCreate();
    }

    @Test
    @SneakyThrows
    void testInsert() {
        OffsetDateTime eventTime = OffsetDateTime.now(ZoneOffset.UTC).truncatedTo(ChronoUnit.MICROS);
        String eventID = UUID.randomUUID().toString();

        var observerMock = mock(StreamObserver.class);
        DefaultDestinationStream underTest = new DefaultDestinationStream(
            observerMock,
            spark,
            config.fullTableName()
        );

        underTest.onNext(testRecord(eventTime, eventID));
        verify(observerMock).onNext(any());
        verify(observerMock, never()).onError(any());

        var foundRecords = readIcebergRecords();
        assertEquals(1, foundRecords.size());
        var record = foundRecords.get(0);
        assertEquals("debug", record.getField("level"));
        assertEquals(eventTime, record.getField("event_time"));
        assertEquals("a debug message", record.getField("message"));
        assertEquals(eventID, record.getField("event_id"));
        assertEquals(123, record.getField("integer_field"));
        assertEquals(Map.of("foo", "bar"), record.getField("map_field"));
    }

    @SneakyThrows
    private List<org.apache.iceberg.data.Record> readIcebergRecords() {
        List<org.apache.iceberg.data.Record> records = new LinkedList<>();

        try (RESTCatalog catalog = new RESTCatalog()) {
            Configuration conf = new Configuration();
            catalog.setConf(conf);
            catalog.initialize("demo", catalogProps);

            Table table = catalog.loadTable(tableId);

            IcebergGenerics.ScanBuilder scanBuilder = IcebergGenerics.read(table);
            try (CloseableIterable<org.apache.iceberg.data.Record> iterable = scanBuilder.build()) {
                iterable.forEach(records::add);
            }
        }

        return records;
    }

    @NotNull
    private Request testRecord(OffsetDateTime eventTime, String eventID) {
        String eventTimeStr = eventTime.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);

        String jsonString = """
                {
                "level": "debug",
                "event_time":  "%s",
                "message": "a debug message",
                "event_id": "%s",
                "integer_field": 123,
                "map_field": {"foo": "bar"}
                }
            """.formatted(eventTimeStr, eventID);

        return Request.newBuilder()
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
            ).build();
    }
}