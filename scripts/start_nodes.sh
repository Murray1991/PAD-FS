#!/bin/bash

DB=./db
CONF=./resources/pad_fs.conf

[ ! -d "$DB" ] && mkdir "$DB"
rm -f $DB/127.0.0.*

screen -d -m -S node1 -t -r java -jar target/pad-fs.jar -b 127.0.0.1 -p $DB -c $CONF
screen -d -m -S node2 -t -r java -jar target/pad-fs.jar -b 127.0.0.2 -p $DB -c $CONF
screen -d -m -S node3 -t -r java -jar target/pad-fs.jar -b 127.0.0.3 -p $DB -c $CONF
screen -d -m -S node4 -t -r java -jar target/pad-fs.jar -b 127.0.0.4 -p $DB -c $CONF
screen -d -m -S node5 -t -r java -jar target/pad-fs.jar -b 127.0.0.5 -p $DB -c $CONF
