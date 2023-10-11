#!/bin/bash

# Generates thrift code for parsing the Parquet format

set -e

mkdir -p parquet_inspect/generated
curl -o parquet.thrift https://raw.githubusercontent.com/apache/parquet-format/master/src/main/thrift/parquet.thrift
thrift -r --gen py -out parquet_inspect/generated parquet.thrift
rm parquet.thrift
