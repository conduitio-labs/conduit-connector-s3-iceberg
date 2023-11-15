# Conduit's connector for S3 and Apache Iceberg

The S3-Apache Iceberg connector is a [Conduit](https://github.com/ConduitIO/conduit) destination connector, 
which makes it possible to write data into S3 in the [Apache Iceberg](https://iceberg.apache.org/) format.

### Pre-requisites
* JDK 17
* Currently, only Unix-like OSes are supported.

## Usage
First, the connector needs to be built, which can be done with `scripts/dist.sh`. A new directory, `dist`,
will be created. The contents of `dist` need to be copied into the Conduit connectors' directory (which, 
by default, is `connectors`). Read more about installing Conduit connectors 
[here](https://conduit.io/docs/connectors/installing).