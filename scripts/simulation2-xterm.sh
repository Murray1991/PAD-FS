#!/bin/bash

DB=./db
CONF12345=./resources/pad_fs.conf
CONF123=./resources/pad_fs1.conf
CONF345=./resources/pad_fs2.conf

[ ! -d "$DB" ] && mkdir "$DB"
rm -f $DB/127.0.0.*

# start the servers
echo "-- start servers with partition [1,2] and [4,5]"
xterm -e "java -jar target/pad-fs.jar -b 127.0.0.1 -p $DB -c $CONF" &
xterm -e "java -jar target/pad-fs.jar -b 127.0.0.2 -p $DB -c $CONF" &
xterm -e "java -jar target/pad-fs.jar -b 127.0.0.3 -p $DB -c $CONF" &
xterm -e "java -jar target/pad-fs.jar -b 127.0.0.4 -p $DB -c $CONF" &
xterm -e "java -jar target/pad-fs.jar -b 127.0.0.5 -p $DB -c $CONF" &

echo "-- wait for the startup"
sleep 6

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
screen -d -m -S node3 -t -r java -jar target/pad-fs.jar -b 127.0.0.3 -p $DB -c $CONF12345

echo "-- wait for convergence"
sleep 20

echo "-- print che concurrent values"
for i in {1..10}
do
        str1=$( java -jar target/pad-fs-cli.jar -g keyc$i )
        echo -e "keyc$i $str1"
done

echo "-- some updates"
for i in {1..10}
do
	java -jar target/pad-fs-cli.jar -p key1$i value11$i
done

for i in {1..10}
do
	java -jar target/pad-fs-cli.jar -p keyc$i valuecUP$i
done

echo "-- wait for convergence"
sleep 20

echo "-- get messages"
for i in {1..10}
do
	str1=$( java -jar target/pad-fs-cli.jar -g key0$i )
	[ "value0$i" != "$str1" ] && echo "error for $str1 != value0$i" && exit 1
	str2=$( java -jar target/pad-fs-cli.jar -g key1$i )
	[ "value11$i" != "$str2" ] && echo "error for $str2 != value11$i" && exit 1
	str3=$( java -jar target/pad-fs-cli.jar -g key2$i )
	[ "value2$i" != "$str3" ] && echo "error for $str3 != value2$i" && exit 1
done

for i in {1..10}
do
        str1=$( java -jar target/pad-fs-cli.jar -g keyc$i )
        [ "valuecUP$i" != "$str1" ] && echo "error for $str1 != valuecUP$i" && exit 1
done

echo "-- well done! :)"

