# Conduit's connector for S3 and Apache Iceberg

The S3-Apache Iceberg connector is a [Conduit](https://github.com/ConduitIO/conduit) destination connector, 
which makes it possible to write data into S3 in the [Apache Iceberg](https://iceberg.apache.org/) format.

## Pre-requisites
* JDK 17
* Currently, only Unix-like OSes are supported.

## Usage
First, the connector needs to be built, which can be done with `scripts/dist.sh`. A new directory, `dist`,
will be created. The contents of `dist` need to be copied into the Conduit connectors' directory (which, 
by default, is `connectors`). Read more about installing Conduit connectors 
[here](https://conduit.io/docs/connectors/installing).

## Configuration

| Field JSON Name        | Description                                         | Required | Default Value | Example                                                                                                                                                                                                    |
|------------------------|-----------------------------------------------------|----------|---------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `catalog.name`         | Catalog name                                        | true     | -             | "exampleCatalog"                                                                                                                                                                                           |
| `namespace`            | Namespace                                           | true     | -             | "exampleNamespace"                                                                                                                                                                                         |
| `table.name`           | Table name                                          | true     | -             | "exampleTable"                                                                                                                                                                                             |
| `s3.endpoint`          | S3 endpoint URL                                     | true     | -             | "https://s3.example.com"                                                                                                                                                                                   |
| `s3.accessKeyId`       | S3 Access Key ID                                    | true     | -             | "exampleAccessKeyId"                                                                                                                                                                                       |
| `s3.secretAccessKey`   | S3 Secret Access Key                                | true     | -             | "exampleSecretKey"                                                                                                                                                                                         |
| `s3.region`            | S3 region                                           | true     | -             | "us-east-1"                                                                                                                                                                                                |
| `catalog.propertyName` | Set a catalog property with the name `propertyName` | true     | -             | {"catalog.uri": "http://localhost:8181"}                                                                                                                                                                   |
| `catalog.catalog-impl` | Catalog implementation to be used                   | true     | -             | "org.apache.iceberg.rest.RESTCatalog" <br/> Possible values: <br/> - "org.apache.iceberg.hadoop.HadoopCatalog" <br/> - "org.apache.iceberg.jdbc.JdbcCatalog" <br/> - "org.apache.iceberg.rest.RESTCatalog" |
