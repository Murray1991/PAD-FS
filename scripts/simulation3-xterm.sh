#!/bin/bash

DB=./db
CONF12345=./resources/pad_fs.conf
CONF123=./resources/pad_fs1.conf
CONF345=./resources/pad_fs2.conf

[ ! -d "$DB" ] && mkdir "$DB"
rm -f $DB/127.0.0.*

# start the servers
echo "-- start servers with partition [1,2] and [4,5]"
xterm -e "java -jar target/pad-fs.jar -b 127.0.0.1 -p $DB -c $CONF123" &
xterm -e "java -jar target/pad-fs.jar -b 127.0.0.2 -p $DB -c $CONF123" &
xterm -e "java -jar target/pad-fs.jar -b 127.0.0.4 -p $DB -c $CONF345" &
xterm -e "java -jar target/pad-fs.jar -b 127.0.0.5 -p $DB -c $CONF345" &

echo "-- wait for the startup"
sleep 10

echo "-- put items in pad-fs"
for i in {1..10}
do
        echo "---- put key0$i , key1$i , key2$i in the fs" 
	java -jar target/pad-fs-cli.jar -p key0$i value0$i
	java -jar target/pad-fs-cli.jar -p key1$i value1$i
	java -jar target/pad-fs-cli.jar -p key2$i value2$i 
done

echo "-- put few items in [1,2] and [4,5]"
for i in {1..10}
do
        java -jar target/pad-fs-cli.jar -p keyc$i valuec1$i -d 127.0.0.1
        java -jar target/pad-fs-cli.jar -p keyc$i valuec4$i -d 127.0.0.4
done


echo "-- wait for convergence"
sleep 10

echo "-- start node3 [127.0.0.3]"
xterm -e "java -jar target/pad-fs.jar -b 127.0.0.3 -p $DB -c $CONF12345" &

sleep 10
