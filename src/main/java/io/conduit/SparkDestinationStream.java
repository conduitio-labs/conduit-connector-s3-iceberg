/*
 * Copyright 2023 Meroxa, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.conduit;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.ByteString;
import com.google.protobuf.Struct;
import com.google.protobuf.util.JsonFormat;
import io.conduit.grpc.Data;
import io.conduit.grpc.Destination;
import io.conduit.grpc.Record;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import lombok.AllArgsConstructor;
import lombok.SneakyThrows;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.conf.ParamType;
import org.jooq.impl.DSL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.table;


/**
 * SparkDestinationStream is a {@link StreamObserver} implementation,
 * which uses a {@link SparkSession} to write records to Iceberg.
 */
@AllArgsConstructor
public class SparkDestinationStream implements StreamObserver<Destination.Run.Request> {
    public static final Logger logger = LoggerFactory.getLogger(SparkDestinationStream.class);
    public static final DSLContext DSL_CONTEXT = DSL.using(SQLDialect.DEFAULT);
    private static final ObjectMapper mapper = new ObjectMapper();

    private final StreamObserver<Destination.Run.Response> responseObserver;

    private final SparkSession spark;
    private final String tableName;

    @Override
    public void onNext(Destination.Run.Request request) {
        try {
            Record rec = request.getRecord();
            doWrite(rec);
            responseObserver.onNext(responseWith(rec.getPosition()));
        } catch (Exception e) {
            logger.error("Couldn't write record.", e);
            responseObserver.onError(
                Status.INTERNAL
                    .withDescription("couldn't write record: " + e.getMessage())
                    .withCause(e)
                    .asException()
            );
        }
    }

    private Destination.Run.Response responseWith(ByteString position) {
        return Destination.Run.Response
            .newBuilder()
            .setAckPosition(position)
            .build();
    }

    private void doWrite(Record rec) {
        switch (rec.getOperation()) {
            case OPERATION_CREATE, OPERATION_SNAPSHOT:
                insertRecord(rec);
                break;
            case OPERATION_UPDATE:
                logger.warn("Updates are not supported yet.");
                break;
            case OPERATION_DELETE:
                deleteRecord(rec);
                break;
            default:
                break;
        }
    }

    @SneakyThrows
    private void deleteRecord(Record rec) {
        var keyMap = toPojoMap(rec.getKey());
        if (CollectionUtils.isEmpty(keyMap)) {
            // prevent deleting all rows
            throw new IllegalArgumentException("key has no fields");
        }

        var queryBuilder = DSL_CONTEXT.delete(table(tableName));
        Condition conditions = null;
        for (Map.Entry<String, Object> e : keyMap.entrySet()) {
            if (conditions == null) {
                conditions = field(e.getKey()).eq(e.getValue());
            } else {
                conditions.and(field(e.getKey()).eq(e.getValue()));
            }
        }

        String deleteQ = queryBuilder.where(conditions).getSQL(ParamType.INLINED);
        spark.sql(deleteQ).show();
    }

    // Transform input `Data` object into a map,
    // where keys are field names and values are POJOs.
    // Boolean, number and string values are supported.
    private Map<String, Object> toPojoMap(Data data) {
        Objects.requireNonNull(data, "cannot transform into POJO map, input is null");

        if (data.hasStructuredData()) {
            return protobufStructToMap(data.getStructuredData());
        }

        return jsonStringToMap(data.getRawData());
    }

    // Transform input `Data` object into a map,
    // where keys are field names and values are POJOs.
    // Assumes that the input is a string representing a valid JSON object.
    private Map<String, Object> jsonStringToMap(ByteString rawData) {
        ObjectNode json;
        try {
            JsonNode jsonNode = mapper.readTree(rawData.toStringUtf8());
            if (!(jsonNode instanceof ObjectNode)) {
                throw new IllegalArgumentException("input data is not JSON");
            }
            json = (ObjectNode) jsonNode;
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("input data is not JSON", e);
        }

        Map<String, Object> map = new HashMap<>();
        json.fields().forEachRemaining(e -> {
            var fieldName = e.getKey();
            var value = e.getValue();
            switch (value.getNodeType()) {
                case BOOLEAN -> map.put(fieldName, value.booleanValue());
                case NUMBER -> map.put(fieldName, value.numberValue());
                case STRING -> map.put(fieldName, value.textValue());
                case NULL, MISSING -> {}
                default -> throw new IllegalArgumentException(
                    "type %s of key field %s is not supported".formatted(fieldName, value.getNodeType())
                );
            }
        });
        return map;
    }

    // Transform input `Struct` object into a map,
    // where keys are field names and values are POJOs.
    private Map<String, Object> protobufStructToMap(Struct data) {
        Map<String, Object> map = new HashMap<>();

        data.getFieldsMap().forEach((fieldName, val) -> {
            String s;
            switch (val.getKindCase()) {
                case NUMBER_VALUE -> s = String.valueOf(val.getNumberValue());
                case STRING_VALUE -> s = val.getStringValue();
                case BOOL_VALUE -> s = String.valueOf(val.getBoolValue());
                default -> throw new IllegalArgumentException(
                    "type %s of key field %s is not supported".formatted(fieldName, val.getKindCase())
                );
            }
            map.put(fieldName, s);
        });

        return map;
    }

    @SneakyThrows
    private void insertRecord(Record rec) {
        Objects.requireNonNull(rec, "record is null");

        logger.trace("inserting record with key: {}", rec.getKey());
        var schema = spark.read().table(tableName).schema();

        String afterString = toJsonString(rec.getPayload().getAfter());
        logger.trace("payload string: {}", afterString);

        Dataset<Row> data = spark.read()
            // set the parsing mode to FAILFAST, so that an exception is thrown
            // when a corrupted record is found.
            // Ref: https://spark.apache.org/docs/latest/sql-data-sources-json.html#data-source-option
            .option("mode", "FAILFAST")
            .schema(schema)
            .json(spark.createDataset(List.of(afterString), Encoders.STRING()));

        data.write()
            .format("iceberg")
            .mode(SaveMode.Append)
            .saveAsTable(tableName);

        logger.trace("done writing");
    }

    // The JSON data may have floating point numbers that are meant to be integers.
    // This can be due to:
    // (1) JSON having a single number type
    // (2) Protobuf having a single number type
    // However, Spark is not able to convert these into an integer, even when using a schema.
    // That's why we're doing that manually here.
    // Also see: https://stackoverflow.com/q/77493625/1059744
    @SneakyThrows
    private String toJsonString(Data data) {
        String jsonStr;
        if (data.hasRawData()) {
            jsonStr = data.getRawData().toStringUtf8();
        } else {
            jsonStr = JsonFormat.printer().print(data.getStructuredData());
        }

        ObjectNode json = (ObjectNode) mapper.readTree(jsonStr);
        json.fieldNames().forEachRemaining(field -> {
            JsonNode value = json.get(field);
            if (value.canConvertToExactIntegral()) {
                json.put(field, value.intValue());
            }
        });

        return mapper.writeValueAsString(json);
    }

    @Override
    public void onError(Throwable t) {
        logger.error("Experienced an error.", t);
        responseObserver.onError(
            Status.INTERNAL.withDescription("Error: " + t.getMessage()).withCause(t).asException()
        );
    }

    @Override
    public void onCompleted() {
        logger.info("Completed.");
        responseObserver.onCompleted();
    }
}
