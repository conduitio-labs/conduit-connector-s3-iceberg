/*
 * Copyright 2022 Meroxa, Inc.
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

import com.google.protobuf.ByteString;
import io.conduit.grpc.Destination;
import io.conduit.grpc.Record;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import lombok.AllArgsConstructor;
import lombok.SneakyThrows;
import org.apache.iceberg.spark.SparkWriteOptions;
import org.apache.spark.sql.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@AllArgsConstructor
public class DefaultDestinationStream implements StreamObserver<Destination.Run.Request> {
    public static final Logger logger = LoggerFactory.getLogger(DefaultDestinationStream.class);

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
    private void insertRecord(Record rec) {
        Objects.requireNonNull(rec, "record is null");
        var schema = spark.read().table(tableName).schema();

        // todo handle structured data as well
        String afterString = rec.getPayload().getAfter().getRawData().toStringUtf8();

        Dataset<Row> data = spark.read()
                .schema(schema)
                .json(spark.createDataset(List.of(afterString), Encoders.STRING()));

        data.write()
                .format("iceberg")
                .mode(SaveMode.Append)
                .saveAsTable(tableName);
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
