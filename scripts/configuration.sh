#!/bin/bash

# needed if local test is performed
# comment, otherwise
BIND_OPT=-b

# server port for --dest, otherwise 8080 is the default
port=9090

# configuration files to set
## for the server
PAD_CONF=resources/pad_fs.conf
PAD_CONF_12345=./resources/pad_fs.conf
PAD_CONF_123=./resources/pad_fs1.conf
PAD_CONF_345=./resources/pad_fs2.conf
## for the client
CONF=resources/nodes.conf

# directories
WORK_DIR=/home/leonardo/git/PAD-FS/
LOG_DIR=log
DB_DIR=db

# put here node addresses/hostnames
node[1]=127.0.0.1
node[2]=127.0.0.2
node[3]=127.0.0.3
node[4]=127.0.0.4
node[5]=127.0.0.5

# start and end
start=1
end=4

# "build" strings
ENTER="cd $WORK_DIR ;"
MKDIR="mkdir -p $DB_DIR ; mkdir -p $LOG_DIR ;"
CLEAN="rm $DB_DIR/* 2> /dev/null ; rm $LOG_DIR/* 2> /dev/null ;"

# strings to execute
BUILD="$ENTER $MKDIR $CLEAN"

function start {
    echo "start node[$1]"
    PADFS="java -jar target/pad-fs.jar -p $DB_DIR -c $PAD_CONF --active"
    [ "$BIND_OPT" == "-b" ] && echo "$BIND_OPT option provided" && EXE="$PADFS -b ${node[$1]}"
    #FILE_LOG="-L log/node$1.log"
    SCREEN="screen $FILE_LOG -d -m -S node$1 -t -r $EXE"
    
    ssh ${node[$1]} "$BUILD $SCREEN"
}

function shutdown {
    echo "shutdown node[$1]"
    ssh ${node[$1]} "screen -S node$1 -p 0 -X quit"
    #ssh ${node[$1]} "screen -S node$1 -p 0 -X stuff "^D""
}

function startFromTo {
    for i in `seq $1 $2`
    do
        start $i
    done
}

function shutdownFromTo {
    for i in `seq $1 $2`
    do
        shutdown $i
    done
}
