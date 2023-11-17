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

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.ByteString;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SparkDestinationStream is a {@link StreamObserver} implementation,
 * which uses a {@link SparkSession} to write records to Iceberg.
 */
@AllArgsConstructor
public class SparkDestinationStream implements StreamObserver<Destination.Run.Request> {
    public static final Logger logger = LoggerFactory.getLogger(SparkDestinationStream.class);
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
        String deleteQ = "DELETE FROM " + tableName + " WHERE ";
        // todo: check if key is not structured
        var mp = rec.getKey().getStructuredData().getFieldsMap();

        String condition = mp.entrySet()
            .stream()
            .map(entry -> entry.getKey() + "=" + entry.getValue().getStringValue())
            .collect(Collectors.joining(" AND "));
        deleteQ += condition;

        spark.sql(deleteQ).show();
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
