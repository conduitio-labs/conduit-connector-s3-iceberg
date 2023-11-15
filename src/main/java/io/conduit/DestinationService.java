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

import java.util.Map;

import io.conduit.grpc.Destination;
import io.conduit.grpc.Destination.Teardown;
import io.conduit.grpc.DestinationPluginGrpc;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A gRPC service exposing destination plugin methods.
 */
public class DestinationService extends DestinationPluginGrpc.DestinationPluginImplBase {
    public static final Logger logger = LoggerFactory.getLogger(DestinationService.class);

    private DefaultDestinationStream runStream;
    Map<String, String> properties;
    RESTCatalog catalog;
    Namespace namespace;
    TableIdentifier tableId;
    private DestinationConfig config;
    private SparkSession spark;

    @Override
    public void configure(Destination.Configure.Request request, StreamObserver<Destination.Configure.Response> responseObserver) {
        logger.info("Configuring the destination.");
        try {
            // the returned config map is unmodifiable, so we make a copy
            // since we need to remove some keys
            config = DestinationConfig.fromMap(request.getConfigMap());
            logger.info("Done configuring the destination.");

            responseObserver.onNext(Destination.Configure.Response.newBuilder().build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            logger.error("Error while configuring destination.", e);
            responseObserver.onError(
                Status.INTERNAL
                    .withDescription("couldn't configure task: " + e.getMessage())
                    .withCause(e)
                    .asException()
            );
        }
    }


    @Override
    public void start(Destination.Start.Request request, StreamObserver<Destination.Start.Response> responseObserver) {
        logger.info("Starting the destination.");

        try {
            logger.info("Setting up a spark session.");
            setupSpark();

            logger.info("Destination started.");

            responseObserver.onNext(Destination.Start.Response.newBuilder().build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            logger.error("Error while starting Connector.", e);
            responseObserver.onError(
                Status.INTERNAL.withDescription("couldn't start connector: " + e.getMessage()).withCause(e).asException()
            );
        }
    }

    private void setupSpark() {
        String catalogName = config.getCatalogName();

        logger.info("setting up spark builder");
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

        logger.info("adding catalog properties to builder");
        config.getCatalogProperties().forEach((k, v) -> {
            // keys are in the form of catalog.propertyName
            builder.config(prefix + "." + k.replaceFirst("catalog.", ""), v);
        });

        logger.info("get spark session");
        try {
            spark = builder.getOrCreate();
        } catch (Throwable e) {
            logger.error("couldn't get spark session", e);
        }
        logger.info("spark session: {}", spark.conf().getAll());
        logger.info("after try catch");
    }

    @Override
    public StreamObserver<Destination.Run.Request> run(StreamObserver<Destination.Run.Response> responseObserver) {
        this.runStream = new DefaultDestinationStream(responseObserver, spark, config.fullTableName());
        return runStream;
    }

    @Override
    public void stop(Destination.Stop.Request request, StreamObserver<Destination.Stop.Response> responseObserver) {
        runStream.onCompleted();
        responseObserver.onNext(Destination.Stop.Response.newBuilder().build());
        responseObserver.onCompleted();
    }

    @Override
    public void teardown(Teardown.Request request, StreamObserver<Teardown.Response> responseObserver) {
        logger.info("Tearing down...");
        try {
            if (spark != null) {
                spark.stop();
            }
            responseObserver.onNext(Teardown.Response.newBuilder().build());
            responseObserver.onCompleted();
            logger.info("Torn down.");
        } catch (Exception e) {
            logger.error("Couldn't tear down.", e);
            responseObserver.onError(
                Status.INTERNAL.withDescription("Couldn't tear down: " + e.getMessage()).withCause(e).asException()
            );
        }
    }
}
