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
import java.util.stream.Collectors;

import io.conduit.grpc.Specifier;
import io.conduit.grpc.Specifier.Parameter;
import io.conduit.grpc.Specifier.Parameter.Validation;
import io.conduit.grpc.Specifier.Specify.Request;
import io.conduit.grpc.Specifier.Specify.Response;
import io.conduit.grpc.SpecifierPluginGrpc;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.regions.Region;

/**
 * A gRPC service exposing this connector's specification.
 */
@Slf4j
public class SpecifierService extends SpecifierPluginGrpc.SpecifierPluginImplBase {
    private static final Validation requiredValidation = Validation.newBuilder()
        .setType(Validation.Type.TYPE_REQUIRED)
        .build();
    private static final List<String> AVAILABLE_CATALOG_IMPL = List.of(
        "org.apache.iceberg.hadoop.HadoopCatalog",
        "org.apache.iceberg.jdbc.JdbcCatalog",
        "org.apache.iceberg.rest.RESTCatalog"
    );

    @Override
    public void specify(Request request, StreamObserver<Response> responseObserver) {
        responseObserver.onNext(
            Response.newBuilder()
                .setName("s3-iceberg")
                .setSummary("An S3 destination plugin for Conduit, written in Java.")
                .setVersion("v0.1.0")
                .setAuthor("Meroxa, Inc.")
                .putAllDestinationParams(buildDestinationParams())
                .build()
        );
        responseObserver.onCompleted();
    }

    private Map<String, Specifier.Parameter> buildDestinationParams() {
        Map<String, Parameter> params = new HashMap<>();
        params.put(
            "catalog.name",
            Specifier.Parameter.newBuilder()
                .setDescription("Catalog name")
                .setType(Specifier.Parameter.Type.TYPE_STRING)
                .addValidations(requiredValidation)
                .build()
        );
        params.put(
            "catalog.catalog-impl",
            Specifier.Parameter.newBuilder()
                .setDescription("Catalog implementation")
                .setType(Specifier.Parameter.Type.TYPE_STRING)
                .addValidations(requiredValidation)
                .addValidations(availableCatalogImpl())
                .build()
        );
        params.put(
            "namespace",
            Specifier.Parameter.newBuilder()
                .setDescription("Namespace")
                .setType(Specifier.Parameter.Type.TYPE_STRING)
                .addValidations(requiredValidation)
                .build()
        );
        params.put(
            "table.name",
            Specifier.Parameter.newBuilder()
                .setDescription("Table name")
                .setType(Specifier.Parameter.Type.TYPE_STRING)
                .addValidations(requiredValidation)
                .build()
        );
        params.put(
            "s3.endpoint",
            Specifier.Parameter.newBuilder()
                .setDescription("S3 endpoint")
                .setType(Specifier.Parameter.Type.TYPE_STRING)
                .addValidations(requiredValidation)
                .build()
        );
        params.put(
            "s3.accessKeyId",
            Specifier.Parameter.newBuilder()
                .setDescription("S3 Access Key ID")
                .setType(Specifier.Parameter.Type.TYPE_STRING)
                .addValidations(requiredValidation)
                .build()
        );
        params.put(
            "s3.secretAccessKey",
            Specifier.Parameter.newBuilder()
                .setDescription("S3 Secret Access Key")
                .setType(Specifier.Parameter.Type.TYPE_STRING)
                .addValidations(requiredValidation)
                .build()
        );
        params.put(
            "s3.region",
            Specifier.Parameter.newBuilder()
                .setDescription("S3 Region")
                .setType(Specifier.Parameter.Type.TYPE_STRING)
                .addValidations(requiredValidation)
                .addValidations(availableS3Regions())
                .build()
        );

        return params;
    }

    private Validation availableS3Regions() {
        return Validation.newBuilder()
            .setType(Validation.Type.TYPE_INCLUSION)
            .setValue(Region.regions().stream().map(Region::toString).collect(Collectors.joining(",")))
            .build();
    }

    private Validation availableCatalogImpl() {
        return Validation.newBuilder()
            .setType(Validation.Type.TYPE_INCLUSION)
            .setValue(String.join(",", AVAILABLE_CATALOG_IMPL))
            .build();
    }
}
