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

import java.util.HashMap;
import java.util.Map;

import io.conduit.grpc.Specifier;
import io.conduit.grpc.Specifier.Parameter;
import io.conduit.grpc.Specifier.Parameter.Validation;
import io.conduit.grpc.Specifier.Specify.Request;
import io.conduit.grpc.Specifier.Specify.Response;
import io.conduit.grpc.SpecifierPluginGrpc;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A gRPC service exposing this connector's specification.
 */
public class SpecifierService extends SpecifierPluginGrpc.SpecifierPluginImplBase {
    public static final Logger logger = LoggerFactory.getLogger(SpecifierService.class);
    private static final Validation requiredValidation = Validation.newBuilder()
        .setType(Validation.Type.TYPE_REQUIRED)
        .build();

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
            DestinationConfig.KEY_CATALOG_NAME,
            Specifier.Parameter.newBuilder()
                .setDescription("Catalog name")
                .setDefault("")
                .setType(Specifier.Parameter.Type.TYPE_STRING)
                .addValidations(requiredValidation)
                .build()
        );
        params.put(
            DestinationConfig.KEY_NAMESPACE,
            Specifier.Parameter.newBuilder()
                .setDescription("Namespace")
                .setDefault("")
                .setType(Specifier.Parameter.Type.TYPE_STRING)
                .addValidations(requiredValidation)
                .build()
        );
        params.put(
            DestinationConfig.KEY_TABLE_NAME,
            Specifier.Parameter.newBuilder()
                .setDescription("Table name")
                .setDefault("")
                .setType(Specifier.Parameter.Type.TYPE_STRING)
                .addValidations(requiredValidation)
                .build()
        );

        return params;
    }
}
