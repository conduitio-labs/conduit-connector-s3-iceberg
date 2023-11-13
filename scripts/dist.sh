#!/bin/bash
mvn clean package

TO_DIR=dist/
rm -rf $TO_DIR
mkdir -p $TO_DIR
cp scripts/conduit-connector-s3-iceberg $TO_DIR
cp target/conduit-connector-s3-iceberg-*.jar $TO_DIR/libs
