#!/bin/bash

DB=./db
CONF=./resources/pad_fs.conf

[ ! -d "$DB" ] && mkdir "$DB"
rm -f $DB/127.0.0.*

# start the servers
xterm -e "java -jar target/pad-fs.jar -b 127.0.0.1 -p $DB -c $CONF" &
xterm -e "java -jar target/pad-fs.jar -b 127.0.0.2 -p $DB -c $CONF" &
xterm -e "java -jar target/pad-fs.jar -b 127.0.0.3 -p $DB -c $CONF" &
xterm -e "java -jar target/pad-fs.jar -b 127.0.0.4 -p $DB -c $CONF" &
xterm -e "java -jar target/pad-fs.jar -b 127.0.0.5 -p $DB -c $CONF" &

echo "-- wait for the startup"
sleep 6

echo "-- put items in pad-fs"
for i in {1..50}
do
	java -jar target/pad-fs-cli.jar -p key0$i value0$i &
	java -jar target/pad-fs-cli.jar -p key1$i value1$i &
	java -jar target/pad-fs-cli.jar -p key2$i value2$i 
done

echo "-- simulate a temporary failure on node4 [127.0.0.4]"
screen -S node4 -p 0 -X stuff "^D"

echo "-- wait for convergence"
sleep 10

echo "-- some updates"
for i in {1..50}
do
	java -jar target/pad-fs-cli.jar -p key1$i value11$i &
done

echo "-- wait for convergence"
sleep 10;

echo "-- restart node4 [127.0.0.4]"
screen -d -m -S node4 -t -r java -jar target/pad-fs.jar -b 127.0.0.4 -p $DB -c $CONF

echo "-- wait for convergence"
sleep 20

echo "-- get messages"
for i in {1..50}
do
	str1=$( java -jar target/pad-fs-cli.jar -g key0$i )
	[ "value0$i" != "$str1" ] && echo "error for $str1 != value0$i" && exit 1
	str2=$( java -jar target/pad-fs-cli.jar -g key1$i )
	[ "value11$i" != "$str2" ] && echo "error for $str2 != value11$i" && exit 1
	str3=$( java -jar target/pad-fs-cli.jar -g key2$i )
	[ "value2$i" != "$str3" ] && echo "error for $str3 != value2$i" && exit 1
done
echo "-- well done! :)"

