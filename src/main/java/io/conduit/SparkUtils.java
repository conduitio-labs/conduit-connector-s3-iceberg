package io.conduit;

import org.apache.spark.sql.SparkSession;

public final class SparkUtils {
    private SparkUtils() {

    }

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
