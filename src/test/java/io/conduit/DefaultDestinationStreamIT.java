package io.conduit;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.google.protobuf.ByteString;
import com.google.protobuf.ListValue;
import java.util.*;

import com.google.protobuf.ByteString;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

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
        Types.NestedField.required(1, "string_field", Types.StringType.get()),
        Types.NestedField.required(2, "timestamp_tz_field", Types.TimestampType.withZone()),
        Types.NestedField.optional(3, "list_field", Types.ListType.ofRequired(100, Types.StringType.get())),
        Types.NestedField.optional(4, "integer_field", Types.IntegerType.get()),
        Types.NestedField.optional(5, "float_field", Types.FloatType.get()),
        Types.NestedField.optional(6, "map_field", Types.MapType.ofOptional(200, 300, Types.StringType.get(), Types.StringType.get())),
        Types.NestedField.optional(7, "integer_in_float_field", Types.FloatType.get()),
        Types.NestedField.optional(8, "missing_field", Types.StringType.get())
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

    @AfterEach
    @SneakyThrows
    void tearDown() {
        try (RESTCatalog catalog = new RESTCatalog()) {
            Configuration conf = new Configuration();
            catalog.setConf(conf);
            catalog.initialize("demo", catalogProps);

            catalog.dropTable(tableId);
        }
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

            // insert some record into the table
            String insertQ = "INSERT INTO "
                    + config.getCatalogName() + "." + config.getNamespace() + "." + config.getTableName()
                    + " VALUES "
                    + "('info', timestamp 'today', 'an info message', array('trace 1'), 'id1', 123, map('bar','baz')) , "
                    + "('error', timestamp 'today', 'an error message', array('trace 2'), 'id2', 456, map('baz','foo'));";
            spark.sql(insertQ).show();
        }
    }

    private SparkSession initSpark() {
        String catalogName = config.getCatalogName();

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
            .config("spark.sql.defaultCatalog", catalogName);

        config.getCatalogProperties().forEach((k, v) -> {
            // keys are in the form of catalog.propertyName
            builder.config(prefix + "." + k.replaceFirst("catalog.", ""), v);
        });

        return builder.getOrCreate();
    }

    @Test
    @SneakyThrows
    void testInsertRaw() {
        OffsetDateTime eventTime = OffsetDateTime.now(ZoneOffset.UTC).truncatedTo(ChronoUnit.MICROS);

        var observerMock = mock(StreamObserver.class);
        DefaultDestinationStream underTest = new DefaultDestinationStream(
            observerMock,
            spark,
            config.fullTableName()
        );

        underTest.onNext(makeRawRecord(eventTime));
        verify(observerMock).onNext(any());
        verify(observerMock, never()).onError(any());

        var foundRecords = readIcebergRecords();
        assertEquals(3, foundRecords.size());
        assertOk(foundRecords.get(2), eventTime);
    }

    @Test
    @SneakyThrows
    void testInsertStructured() {
        OffsetDateTime eventTime = OffsetDateTime.now(ZoneOffset.UTC).truncatedTo(ChronoUnit.MICROS);

        var observerMock = mock(StreamObserver.class);
        DefaultDestinationStream underTest = new DefaultDestinationStream(
            observerMock,
            spark,
            config.fullTableName()
        );

        underTest.onNext(makeStructuredRecord(eventTime));
        verify(observerMock).onNext(any());
        verify(observerMock, never()).onError(any());

        var foundRecords = readIcebergRecords();
        assertEquals(3, foundRecords.size());
        assertOk(foundRecords.get(2), eventTime);
    }

    @Test
    void testDelete() {
        var observerMock = Mockito.mock(StreamObserver.class);
        DefaultDestinationStream stream = new DefaultDestinationStream(observerMock, spark, config.getCatalogName() + "." + config.getNamespace() + "." + config.getTableName());
        stream.onNext(
                Request.newBuilder()
                        .setRecord(Record.newBuilder()
                                .setKey(
                                        Data.newBuilder()
                                                .setStructuredData(Struct.newBuilder()
                                                        .putFields("integer_field", Value.newBuilder()
                                                                .setStringValue("123")
                                                                .build())
                                                        .build())
                                ).setOperation(Operation.OPERATION_DELETE)
                                .build()
                        ).build()
        );
        var foundRecords = readIcebergRecords();
        assertEquals(1, foundRecords.size());
        // assert more
    }

    private void assertOk(org.apache.iceberg.data.Record record, OffsetDateTime eventTime) {
        assertEquals("debug", record.getField("string_field"));
        assertEquals(eventTime, record.getField("timestamp_tz_field"));
        assertEquals(123, record.getField("integer_field"));
        assertEquals(456.78f, record.getField("float_field"));
        assertEquals(987f, record.getField("integer_in_float_field"));
        assertEquals(List.of("item_1", "item_2"), record.getField("list_field"));
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

        // sort the records depending on the "integer_field"
        records.sort(Comparator.comparingInt(record -> (Integer) record.getField("integer_field")));
        return records;
    }

    @NotNull
    private Request makeRawRecord(OffsetDateTime eventTime) {
        String eventTimeStr = eventTime.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);

        String jsonString = """
                {
                "string_field": "debug",
                "timestamp_tz_field":  "%s",
                "integer_field": 123,
                "float_field": 456.78,
                "integer_in_float_field": 987,
                "list_field": ["item_1", "item_2"],
                "map_field": {"foo": "bar"}
                }
            """.formatted(eventTimeStr);

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

    @NotNull
    private Request makeStructuredRecord(OffsetDateTime eventTime) {
        String eventTimeStr = eventTime.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);

        return Request.newBuilder()
            .setRecord(Record.newBuilder()
                .setPayload(
                    Change.newBuilder()
                        .setAfter(
                            Data.newBuilder()
                                .setStructuredData(Struct.newBuilder()
                                    .putFields("string_field", Value.newBuilder().setStringValue("debug").build())
                                    .putFields("timestamp_tz_field", Value.newBuilder().setStringValue(eventTimeStr).build())
                                    .putFields("integer_field", Value.newBuilder().setNumberValue(123).build())
                                    .putFields("float_field", Value.newBuilder().setNumberValue(456.78).build())
                                    .putFields("integer_in_float_field", Value.newBuilder().setNumberValue(987).build())
                                    .putFields(
                                        "map_field",
                                        Value.newBuilder().setStructValue(
                                            Struct.newBuilder()
                                                .putFields("foo", Value.newBuilder().setStringValue("bar").build())
                                                .build()
                                        ).build()
                                    )
                                    .putFields(
                                        "list_field",
                                        Value.newBuilder().setListValue(
                                            ListValue.newBuilder()
                                                .addValues(Value.newBuilder().setStringValue("item_1").build())
                                                .addValues(Value.newBuilder().setStringValue("item_2").build())
                                                .build()
                                        ).build()
                                    ).build()
                                ).build()
                        ).build()
                ).setOperation(Operation.OPERATION_CREATE)
                .build()
            ).build();
    }
}
