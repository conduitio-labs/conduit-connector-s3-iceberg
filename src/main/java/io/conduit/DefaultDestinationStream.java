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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.ByteString;
import io.conduit.grpc.Destination;
import io.conduit.grpc.Record;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import lombok.AllArgsConstructor;
import lombok.SneakyThrows;
import org.apache.iceberg.spark.SparkWriteOptions;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@AllArgsConstructor
public class DefaultDestinationStream implements StreamObserver<Destination.Run.Request> {
    public static final Logger logger = LoggerFactory.getLogger(DefaultDestinationStream.class);

    private final StreamObserver<Destination.Run.Response> responseObserver;
    private SparkSession spark;
    private String tableName;

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
        logger.info("DO WRITE..");
        logger.info(rec.toString());
        switch (rec.getOperation()) {
            case OPERATION_CREATE, OPERATION_SNAPSHOT:
                insertRecord(rec);
                break;
            case OPERATION_UPDATE:
                break;
            case OPERATION_DELETE:
                break;
            default:
                break;
        }
    }

    @SneakyThrows
    private void insertRecord(Record rec) {
        var schema = spark.read().table(tableName).schema();

        String afterString = rec.getPayload().getAfter().getRawData().toStringUtf8();
        logger.info("payload string: {}", afterString);

        JsonNode afterJson = new ObjectMapper().readTree(afterString);
        logger.info("afterJson full: {}", afterJson);
        logger.info("afterJson level: {}", afterJson.get("level").asText());

        Dataset<Row> data = spark.read()
            .schema(schema)
            .json(spark.createDataset(List.of(afterString), Encoders.STRING()));

        data.write()
            .format("iceberg")
            .mode("append")
            .option(SparkWriteOptions.CHECK_NULLABILITY, false)
            .option(SparkWriteOptions.CHECK_ORDERING, false)
            .saveAsTable(tableName);
        logger.info("done writing");

        String selectQ = "SELECT * FROM " + tableName;
        spark.sql(selectQ).show();
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
