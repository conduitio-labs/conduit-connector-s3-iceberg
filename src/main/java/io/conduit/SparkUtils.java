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

import org.apache.spark.sql.SparkSession;

/**
 * A collection of utility methods working with Spark.
 */
public final class SparkUtils {
    private SparkUtils() {

    }

    /**
     * Creates a new {@link SparkSession} instance.
     *
     *@param name Spark application name
     *@param config A {@link DestinationConfig} to be used for configuring the SparkSession.
     *@return A new {@link SparkSession} instance.
     */
    public static SparkSession create(String name, DestinationConfig config) {
        String catalogName = config.getCatalogName();
        System.setProperty("aws.region", config.getS3Region());

        String prefix = "spark.sql.catalog." + catalogName;
        var builder = SparkSession
            .builder()
            .master("local[*]")
            .appName(name)
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
}
