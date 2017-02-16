#!/bin/bash

# needed if local test is performed
# comment, otherwise
BIND_OPT=-b

# server port for --dest, otherwise 8080 is the default
port=9090

# configuration files to set
## for the server
PAD_CONF=resources/pad_fs.conf
PAD_CONF_1234=./resources/pad_fs.conf
PAD_CONF_123=./resources/pad_fs1.conf
PAD_CONF_34=./resources/pad_fs2.conf
## for the client
CONF=resources/nodes.conf

# directories
WORK_DIR=/home/leonardo/git/PAD-FS/

# put here node addresses/hostnames
node[1]=127.0.0.1
node[2]=127.0.0.2
node[3]=127.0.0.3
node[4]=127.0.0.4

# start and end
start=1
end=4

function start {
    echo "start node[$1]"
    extra="-b ${node[$1]}"
    exe="cd $WORK_DIR ; screen  -d -m -S node$1 -t -r ./launch.sh $PAD_CONF '$extra'"
    ssh ${node[$1]} ""$exe""
}

function shutdown {
    echo "shutdown node[$1]"
    stop="screen -S node$1 -p 0 -X quit"
    ssh ${node[$1]} ""$stop""
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
