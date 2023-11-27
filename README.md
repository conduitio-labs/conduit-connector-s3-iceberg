# Conduit's connector for S3 and Apache Iceberg

The S3-Apache Iceberg connector is a [Conduit](https://github.com/ConduitIO/conduit) destination connector, 
which makes it possible to write data into S3 in the [Apache Iceberg](https://iceberg.apache.org/) format.

## Pre-requisites
* JDK 17
* Currently, only Unix-like OSes are supported.

## How to build
First, the connector needs to be built, which can be done with `scripts/dist.sh`. A new directory, `dist`,
will be created. The contents of `dist` need to be copied into the Conduit connectors' directory (which, 
by default, is `connectors`). Read more about installing Conduit connectors 
[here](https://conduit.io/docs/connectors/installing).

## Destination
This destination connector pushes data from upstream resources to an S3 iceberg table via Conduit.

For inserts, a record is inserted into the table, which will append an iceberg formatted file into S3. For Deletes, 
the `Record.Key` field from the OpenCDC record will be used as a condition for deleting a record. Finally, for Updates,
the record will first be deleted according to its `Record.Key` value, then the updated version will be inserted.

## Configuration

| Field JSON Name        | Description                                         | Required | Default Value | Example                                                                                                                                                                                                            |
|------------------------|-----------------------------------------------------|----------|---------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `catalog.name`         | Catalog name                                        | true     | -             | "exampleCatalog"                                                                                                                                                                                                   |
| `namespace`            | Namespace                                           | true     | -             | "exampleNamespace"                                                                                                                                                                                                 |
| `table.name`           | Table name                                          | true     | -             | "exampleTable"                                                                                                                                                                                                     |
| `s3.endpoint`          | S3 endpoint URL                                     | true     | -             | "https://s3.example.com"                                                                                                                                                                                           |
| `s3.accessKeyId`       | S3 Access Key ID                                    | true     | -             | "exampleAccessKeyId"                                                                                                                                                                                               |
| `s3.secretAccessKey`   | S3 Secret Access Key                                | true     | -             | "exampleSecretKey"                                                                                                                                                                                                 |
| `s3.region`            | S3 region                                           | true     | -             | "us-east-1"                                                                                                                                                                                                        |
| `catalog.catalog-impl` | Catalog implementation to be used                   | true     | -             | `"org.apache.iceberg.rest.RESTCatalog"` <br/> Possible values: <br/> - `"org.apache.iceberg.hadoop.HadoopCatalog"` <br/> - `"org.apache.iceberg.jdbc.JdbcCatalog"` <br/> - `"org.apache.iceberg.rest.RESTCatalog"` |
| `catalog.{propertyName}` | Set a catalog property with the name `propertyName` | true     | -             | {"catalog.uri": "http://localhost:8181"}                                                                                                                                                                           |

## Example pipeline configuration file
```yaml
---
version: 2.0
pipelines:
 - id: postgres-to-s3-iceberg
   status: running
   description: Postgres to S3 Iceberg
   connectors:
     - id: pg-source
       type: source
       plugin: "builtin:postgres"
       name: source1
       settings:
         url: "postgresql://username:password@localhost/dbname?sslmode=disable"
         key: id
         table: "iceberg_input"
         snapshotMode: "never"
         cdcMode: "logrepl"
     - id: s3-iceberg-destination
       type: destination
       plugin: standalone:s3-iceberg
       name:  destination1
       settings:
         namespace: "conduit"
         table.name: "test_table"
         catalog.name: "demo"
         catalog.catalog-impl: "org.apache.iceberg.rest.RESTCatalog"
         catalog.uri: "http://localhost:8181"
         s3.endpoint: "http://localhost:9000"
         s3.accessKeyId: "admin"
         s3.secretAccessKey: "password"
         s3.region: "us-east-1"
  ```
check [Pipeline Configuration Files Docs](https://github.com/ConduitIO/conduit/blob/main/docs/pipeline_configuration_files.md)
for more details about how to run this configuration file.

## Known Limitations
In rare occasions, when updating a record (which consists of first deleting and then inserting the updated one),
If an error occurred while inserting the new record, then an Error will be returned, and a record might be lost if the
error happened after the delete operation and before the insert. Check your table's snapshots to recover the data if lost.
