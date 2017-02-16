#!/bin/bash

source scripts/configuration.sh

## start the nodes, function in configuration.sh
startFromTo $start $end

echo "-- wait for the startup"
sleep 5

## start test
echo "-- put 60 items in pad-fs"
for i in {1..20}
do
	java -jar target/pad-fs-cli.jar -p key0$i value0$i -c $CONF
	java -jar target/pad-fs-cli.jar -p key1$i value1$i -c $CONF
	java -jar target/pad-fs-cli.jar -p key2$i value2$i -c $CONF
done

echo "-- shutdown node 2"
shutdown 2

echo "-- update 20 messages"
for i in {1..20}
do
	java -jar target/pad-fs-cli.jar -p key1$i value11$i -c $CONF
done

echo "-- start node 2"
start 2
sleep 2

echo "-- update 20 messages"
for i in {1..20}
do
	java -jar target/pad-fs-cli.jar -p key2$i value22$i -c $CONF
done

echo "-- wait for convergence"
sleep 10

echo "-- get messages"
for i in {1..20}
do
	str1=$( java -jar target/pad-fs-cli.jar -g key0$i -c $CONF )
	[ "value0$i" != "$str1" ] && echo "error for $str1 != value0$i" && exit 1
	str2=$( java -jar target/pad-fs-cli.jar -g key1$i -c $CONF )
	[ "value11$i" != "$str2" ] && echo "error for $str2 != value11$i" && exit 1
	str3=$( java -jar target/pad-fs-cli.jar -g key2$i -c $CONF )
	[ "value22$i" != "$str3" ] && echo "error for $str3 != value22$i" && exit 1
done

echo "-- shutdown node 1"
shutdown 1

echo "-- get messages"
for i in {1..20}
do
	str1=$( java -jar target/pad-fs-cli.jar -g key0$i -c $CONF )
	[ "value0$i" != "$str1" ] && echo "error for $str1 != value0$i" && exit 1
	str2=$( java -jar target/pad-fs-cli.jar -g key1$i -c $CONF )
	[ "value11$i" != "$str2" ] && echo "error for $str2 != value11$i" && exit 1
	str3=$( java -jar target/pad-fs-cli.jar -g key2$i -c $CONF )
	[ "value22$i" != "$str3" ] && echo "error for $str3 != value22$i" && exit 1
done

echo "-- time for get values: $runtime"
shutdownFromTo $start $end

echo "-- well done! :)"
