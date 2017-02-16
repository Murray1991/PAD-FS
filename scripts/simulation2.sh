#!/bin/bash

source scripts/configuration.sh

## start the nodes 1, 2
echo "-- start nodes 1, 2 (they know only 3)"
PAD_CONF=$PAD_CONF_123
startFromTo 1 2

# start the node 4
echo "-- start nodes 4  (it knows only 3)"
PAD_CONF=$PAD_CONF_34
startFromTo 4 4

echo "-- wait for the startup"
sleep 5

echo "-- put items in pad-fs"
for i in {1..10}
do
	java -jar target/pad-fs-cli.jar -p key0$i value0$i -c $CONF
	java -jar target/pad-fs-cli.jar -p key1$i value1$i -c $CONF
	java -jar target/pad-fs-cli.jar -p key2$i value2$i -c $CONF 
done

echo "-- put few new items both in [1,2] and [4]"
for i in {1..10}
do
        java -jar target/pad-fs-cli.jar -p keyc$i valuec1$i -d ${node[1]} $port
        java -jar target/pad-fs-cli.jar -p keyc$i valuec4$i -d ${node[4]} $port
done

echo "-- start node 3 (it knows all the other nodes)"
PAD_CONF=$PAD_CONF_1234
start 3

echo "-- wait for convergence"
sleep 15

echo "-- check if keys with conflicts are present"
for i in {1..10}
do
    var=($(java -jar target/pad-fs-cli.jar -g keyc$i -c $CONF))
    [ "${var[0]}" != "valuec1$i" ] && [ "${var[1]}" != "valuec1$i" ] && echo "error for keyc$i [ ${var[0]}, ${var[1]} ]" && exit 1
    [ "${var[0]}" != "valuec4$i" ] && [ "${var[1]}" != "valuec4$i" ] && echo "error for keyc$i [ ${var[0]}, ${var[1]} ]" && exit 1
done

echo "-- update some values"
for i in {1..10}
do
	java -jar target/pad-fs-cli.jar -p key1$i value11$i -c $CONF
done

echo "-- shutdown node 1"
shutdown 1

echo "-- wait a little"
sleep 3

echo "-- remove the previous updates and the keys with multiple values"
for i in {1..10}
do
	java -jar target/pad-fs-cli.jar -r key1$i -c $CONF
	java -jar target/pad-fs-cli.jar -r keyc$i -c $CONF
done

echo "-- restart node 1"
start 1

echo "-- wait for detection and update"
sleep 15

echo "-- get messages"
for i in {1..10}
do
	str=$( java -jar target/pad-fs-cli.jar -g key0$i -c $CONF )
	[ "value0$i" != "$str" ] && echo "error for $str != value0$i" && exit 1
	
	str=$( java -jar target/pad-fs-cli.jar -g key2$i -c $CONF )
	[ "value2$i" != "$str" ] && echo "error for $str != value2$i" && exit 1
	
	# the message for a string not found
        NOT_FOUND="Your request is not present in the pad-fs system"
        
	str=$( java -jar target/pad-fs-cli.jar -g key1$i -c $CONF )
	[ "$NOT_FOUND" != "$str" ] && echo "key1$i has been found: $str" && exit 1
	
	str=$( java -jar target/pad-fs-cli.jar -g keyc$i -c $CONF )
	[ "$NOT_FOUND" != "$str" ] && echo "keyc$i has been found: $str" && exit 1
done

shutdownFromTo $start $end
echo "-- well done! :)"

