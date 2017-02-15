#!/bin/bash

source scripts/configuration.sh

## start the nodes 1, 2
echo "-- start nodes 1, 2 (they know only 3)"
PAD_CONF=$PAD_CONF_123
startFromTo 1 2

# start the nodes 4, 5
echo "-- start nodes 4, 5 (they know only 3)"
PAD_CONF=$PAD_CONF_345
startFromTo 4 5

echo "-- wait for the startup"
sleep 10

echo "-- put items in pad-fs"
for i in {1..10}
do
	java -jar target/pad-fs-cli.jar -p key0$i value0$i
	java -jar target/pad-fs-cli.jar -p key1$i value1$i
	java -jar target/pad-fs-cli.jar -p key2$i value2$i 
done

echo "-- put few new items both in [1,2] and [4,5]"
for i in {1..10}
do
        java -jar target/pad-fs-cli.jar -p keyc$i valuec1$i -d ${node[1]} $port
        java -jar target/pad-fs-cli.jar -p keyc$i valuec4$i -d ${node[4]} $port
done

echo "-- wait for convergence"
sleep 10

# start the nodes 3
echo "-- start node 3 (it knows all the others)"
PAD_CONF=$PAD_CONF_12345
start 3

echo "-- wait for convergence"
sleep 20

echo "-- check if keys with conflicts are present"
for i in {1..10}
do
    var=($(java -jar target/pad-fs-cli.jar -g keyc$i))
    [ "${var[0]}" != "valuec1$i" ] && [ "${var[1]}" != "valuec1$i" ] && echo "error for [ ${var[0]}, ${var[1]} ]" && exit 1
    [ "${var[0]}" != "valuec4$i" ] && [ "${var[1]}" != "valuec4$i" ] && echo "error for [ ${var[0]}, ${var[1]} ]" && exit 1
done

echo "-- update some values"
for i in {1..10}
do
	java -jar target/pad-fs-cli.jar -p key1$i value11$i
done

echo "-- shutdown node 1"
shutdown 1

echo "-- wait a little"
sleep 6

echo "-- remove the previous updates and the keys with multiple values"
for i in {1..10}
do
	java -jar target/pad-fs-cli.jar -r key1$i
	java -jar target/pad-fs-cli.jar -r keyc$i
done

echo "-- wait few seconds"
sleep 5

echo "-- restart node 1"
start 1

echo "-- wait for convergence"
sleep 20

echo "-- get messages"
for i in {1..10}
do
	str=$( java -jar target/pad-fs-cli.jar -g key0$i )
	[ "value0$i" != "$str" ] && echo "error for $str != value0$i" && exit 1
	
	str=$( java -jar target/pad-fs-cli.jar -g key2$i )
	[ "value2$i" != "$str" ] && echo "error for $str != value2$i" && exit 1
	
	# the message for a string not found
        NOT_FOUND="Your request is not present in the pad-fs system"
        
	str=$( java -jar target/pad-fs-cli.jar -g key1$i )
	[ "$NOT_FOUND" != "$str" ] && echo "key1$i has been found: $str" && exit 1
	
	str=$( java -jar target/pad-fs-cli.jar -g keyc$i )
	[ "$NOT_FOUND" != "$str" ] && echo "keyc$i has been found: $str" && exit 1
done

shutdownFromTo 1 5
echo "-- well done! :)"

