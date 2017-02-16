#!/bin/bash

conf=$1
extra=$2

rm -r db
rm -r log
mkdir -p db
mkdir -p log

# keep "-b $host" only for local tests 
java -jar target/pad-fs.jar -c $conf -p db $extra
