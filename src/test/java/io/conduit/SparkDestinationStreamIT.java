package io.conduit;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.google.protobuf.ByteString;
import com.google.protobuf.ListValue;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import io.conduit.grpc.Change;
import io.conduit.grpc.Data;
import io.conduit.grpc.Destination;
import io.conduit.grpc.Destination.Run.Request;
import io.conduit.grpc.Operation;
import io.conduit.grpc.Record;
import io.grpc.StatusException;
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
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import software.amazon.awssdk.regions.Region;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

class SparkDestinationStreamIT {
    SparkDestinationStream underTest;

    SparkSession spark;
    DestinationConfig config;
    Map<String, String> catalogProps;
    StreamObserver<Destination.Run.Response> observerMock;

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

    Namespace namespace = Namespace.of("conduit");
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
            "s3.accessKeyId", "admin",
            "s3.secretAccessKey", "password",
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

        spark = SparkUtils.create(SparkDestinationStreamIT.class.getName(), config);
        initTable();

        observerMock = mock(StreamObserver.class);
        underTest = new SparkDestinationStream(
            observerMock,
            spark,
            config.fullTableName()
        );
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
        }
    }

    @Test
    @SneakyThrows
    void testInsertRaw() {
        OffsetDateTime eventTime = OffsetDateTime.now(ZoneOffset.UTC).truncatedTo(ChronoUnit.MICROS);

        underTest.onNext(makeRawRecord(eventTime));
        verify(observerMock).onNext(any());
        verify(observerMock, never()).onError(any());

        var foundRecords = readIcebergRecords();
        assertEquals(1, foundRecords.size());
        assertOk(foundRecords.get(0), eventTime);
    }

    @Test
    @SneakyThrows
    void testInsertStructured() {
        OffsetDateTime eventTime = OffsetDateTime.now(ZoneOffset.UTC).truncatedTo(ChronoUnit.MICROS);

        underTest.onNext(makeStructuredRecord(eventTime));
        verify(observerMock).onNext(any());
        verify(observerMock, never()).onError(any());

        var foundRecords = readIcebergRecords();
        assertEquals(1, foundRecords.size());
        assertOk(foundRecords.get(0), eventTime);
    }

    @Test
    void testDeleteStructuredKey() {
        testDeleteWithKey(
            Data.newBuilder()
                .setStructuredData(Struct.newBuilder()
                    .putFields("integer_field", Value.newBuilder()
                        .setNumberValue(12)
                        .build())
                    .build())
                .build()
        );

        verify(observerMock).onNext(any());
        verify(observerMock, never()).onError(any());
        var foundRecords = readIcebergRecords();
        assertEquals(1, foundRecords.size());
        assertEquals(34, foundRecords.get(0).getField("integer_field"));
    }

    @Test
    void testDeleteRawKey_InvalidJSON() {
        testDeleteWithKey(
            Data.newBuilder()
                .setRawData(ByteString.copyFromUtf8("abc123"))
                .build()
        );

        var captor = ArgumentCaptor.forClass(Exception.class);
        verify(observerMock).onError(captor.capture());
        assertInstanceOf(IllegalArgumentException.class, captor.getValue().getCause());
        assertEquals("input data is not JSON", captor.getValue().getCause().getMessage());
    }

    @Test
    void testDeleteRawKey_NullFields() {
        testDeleteWithKey(
            Data.newBuilder()
                .setRawData(ByteString.copyFromUtf8("""
                    {
                      "integer_field": null
                    }
                    """))
                .build()
        );

        var captor = ArgumentCaptor.forClass(Exception.class);
        verify(observerMock).onError(captor.capture());
        assertInstanceOf(IllegalArgumentException.class, captor.getValue().getCause());
        assertEquals("key has no fields", captor.getValue().getCause().getMessage());
    }

    @Test
    void testDeleteRawKey_EmptyJSON() {
        testDeleteWithKey(
            Data.newBuilder()
                .setRawData(ByteString.copyFromUtf8(""))
                .build()
        );

        var captor = ArgumentCaptor.forClass(Exception.class);
        verify(observerMock).onError(captor.capture());
        assertInstanceOf(StatusException.class, captor.getValue());
        assertInstanceOf(IllegalArgumentException.class, captor.getValue().getCause());
        assertEquals("input data is not JSON", captor.getValue().getCause().getMessage());
    }

    @Test
    void testDeleteRawKey_NoFields() {
        testDeleteWithKey(
            Data.newBuilder()
                .setRawData(ByteString.copyFromUtf8("{}"))
                .build()
        );

        var captor = ArgumentCaptor.forClass(Exception.class);
        verify(observerMock).onError(captor.capture());
        assertInstanceOf(IllegalArgumentException.class, captor.getValue().getCause());
        assertEquals("key has no fields", captor.getValue().getCause().getMessage());
    }

    @Test
    void testDeleteStructuredKey_NoFields() {
        testDeleteWithKey(
            Data.newBuilder()
                .setStructuredData(Struct.newBuilder().build())
                .build()
        );

        var captor = ArgumentCaptor.forClass(StatusException.class);
        verify(observerMock).onError(captor.capture());
        assertInstanceOf(IllegalArgumentException.class, captor.getValue().getCause());
        assertEquals("key has no fields", captor.getValue().getCause().getMessage());
    }

    @Test
    void testDeleteRawDataKey() {
        testDeleteWithKey(
            Data.newBuilder()
                .setRawData(ByteString.copyFromUtf8("""
                    {
                      "integer_field": 12
                    }
                    """))
                .build()
        );

        verify(observerMock).onNext(any());
        verify(observerMock, never()).onError(any());
        var foundRecords = readIcebergRecords();
        assertEquals(1, foundRecords.size());
        assertEquals(34, foundRecords.get(0).getField("integer_field"));
    }

    @Test
    void testDeleteWithMaliciousKey() {
        testDeleteWithKey(
            Data.newBuilder()
                .setRawData(ByteString.copyFromUtf8("""
                    {
                      "integer_field": "105 OR 1=1"
                    }
                    """))
                .build()
        );

        verify(observerMock).onNext(any());
        verify(observerMock, never()).onError(any());
        var foundRecords = readIcebergRecords();
        assertEquals(2, foundRecords.size());
    }

    // Inserts two records, with integer_field set to 12 and 32,
    // and then deletes a record with the given key.
    private void testDeleteWithKey(Data key) {
        var observerMock = Mockito.mock(StreamObserver.class);
        insertTestRecord("testDelete_record", 12);
        insertTestRecord("testDelete_record", 34);

        underTest.onNext(
            Request.newBuilder()
                .setRecord(Record.newBuilder()
                    .setKey(key)
                    .setOperation(Operation.OPERATION_DELETE)
                    .build()
                ).build()
        );
    }

    private void insertTestRecord(String stringField, int integerField) {
        String insertQ = """
            INSERT INTO %s
            (string_field, timestamp_tz_field, list_field, integer_field, float_field, integer_in_float_field, map_field, missing_field)
            VALUES
            ('%s', timestamp 'today', array('trace 2'), %d, 87.65, 200, map('baz', 'foo'), 'rainy');
            """.formatted(config.fullTableName(), stringField, integerField);

        spark.sql(insertQ).show();
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
