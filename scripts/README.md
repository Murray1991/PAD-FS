Simple script tests used to simulate the behaviour of the PAD-FS system on the same machine. 

These files have been used for my convenience in order to automatize some tests and some modifications are needed in order to perform the simulations in a real cluster. The file `configuration.sh` needs to be modified:

* *PAD_CONF* variable must contain the name of the configuration file for the servers 
* *PAD_CONF_xxxx* variables are used in *scripts/simulation2.sh* in order to simulate a network partition at the very beginning and see if conflicts are handled correctly. 
* *WORK_DIR* is the path for the directory in which `launch.sh` is located
* *node[x]* contains the x-th node hostname

The root file `launch.sh` is a simple script that starts the pad-fs node, it must reside in the remote node and you can modify it according to your needs.
