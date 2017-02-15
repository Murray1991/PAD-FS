#!/bin/bash

source scripts/configuration.sh

## start the nodes, function in configuration.sh
startFromTo $start $end

echo "-- wait for the startup"
sleep 10

## start test
echo "-- sequence of updates"
for i in {1..5}
do
	java -jar target/pad-fs-cli.jar -p key0 value0$i -c $CONF
	java -jar target/pad-fs-cli.jar -p key1 value1$i -c $CONF
	java -jar target/pad-fs-cli.jar -p key2 value2$i -c $CONF
	java -jar target/pad-fs-cli.jar -p key3 value3$i -c $CONF
	java -jar target/pad-fs-cli.jar -p key4 value4$i -c $CONF
	java -jar target/pad-fs-cli.jar -p key5 value5$i -c $CONF
	java -jar target/pad-fs-cli.jar -p key6 value6$i -c $CONF
	java -jar target/pad-fs-cli.jar -p key7 value7$i -c $CONF
	java -jar target/pad-fs-cli.jar -p key8 value8$i -c $CONF
done

echo "-- shutdown node 1"
shutdown 1

sleep 2

echo "-- remove all keys"
for i in {0..8}
do
	java -jar target/pad-fs-cli.jar -r key$i -c $CONF
done

echo "-- restart node 1"
start 1

echo "-- wait for convergence"
sleep 10

echo "-- check if all values have been removed"
for i in {0..8}
do
	NOT_FOUND="Your request is not present in the pad-fs system"
	str=$( java -jar target/pad-fs-cli.jar -g key$i )
	
	[ "$NOT_FOUND" != "$str" ] && echo "key$i has been found: $str" && exit 1
done

# close properly the servers
shutdownFromTo $start $end

echo "-- well done! :)"
