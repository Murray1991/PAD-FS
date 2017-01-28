#!/bin/bash

# close properly the servers sending EOF signal
screen -S node1 -p 0 -X stuff "^D"
screen -S node2 -p 0 -X stuff "^D"
screen -S node3 -p 0 -X stuff "^D"
screen -S node4 -p 0 -X stuff "^D"
screen -S node5 -p 0 -X stuff "^D"
