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

import com.google.protobuf.ByteString;
import io.conduit.grpc.Destination;
import io.conduit.grpc.Record;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.apache.iceberg.spark.SparkWriteOptions;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class DefaultDestinationStream implements StreamObserver<Destination.Run.Request> {
    public static final Logger logger = LoggerFactory.getLogger(DefaultDestinationStream.class);

    private final StreamObserver<Destination.Run.Response> responseObserver;
    private final SparkSession spark;
    private final String tableName;

    public DefaultDestinationStream(StreamObserver<Destination.Run.Response> responseObserver, DestinationConfig cfg) {
        this.responseObserver = responseObserver;
        this.tableName = cfg.getTableName();
        this.spark = buildSparkSession(cfg);
    }

    private SparkSession buildSparkSession(DestinationConfig cfg) {
        String prefix = "spark.sql.catalog." + cfg.getCatalogName();
        var builder = SparkSession
            .builder()
            .master("local[*]")
            .appName("Java API Demo")
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config(prefix, "org.apache.iceberg.spark.SparkCatalog")
            .config(prefix + ".catalog-impl", "org.apache.iceberg.rest.RESTCatalog")
            .config(prefix + ".io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
            .config(prefix + ".s3.endpoint", cfg.getS3Endpoint())
            .config(prefix + ".s3.access-key-id", cfg.getS3AccessKeyId())
            .config(prefix + ".s3.secret-access-key", cfg.getS3SecretAccessKey())
            .config("spark.sql.defaultCatalog", cfg.getCatalogName());

        cfg.getCatalogProperties()
            .forEach((name, value) -> builder.config(prefix + "." + name, value));

        var session = builder.getOrCreate();
        session.sparkContext().setLogLevel("ERROR");

        return session;
    }

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
        String jsonString = rec.getPayload().getAfter().toByteString().toStringUtf8();
        // needed to properly convert types, e.g. long to int (as in integer_field)
        var schema = spark.read().table(tableName).schema();

        Dataset<Row> data = spark.read()
            .schema(schema)
            .json(spark.createDataset(List.of(jsonString), Encoders.STRING()));

        data.write()
            .format("iceberg")
            .mode("append")
            .option(SparkWriteOptions.CHECK_NULLABILITY, false)
            .option(SparkWriteOptions.CHECK_ORDERING, false)
            .saveAsTable("demo.webapp.logs");
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
