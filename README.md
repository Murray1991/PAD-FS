# PAD-FS: project guidelines and design choices

#### Overview
* A simple but distributed key-value storage system
* Eventually consistent: focus on availability and partition tolerance
* Robust to network partitions and temporary failures
* Simple API: put, get, list, remove
* CLI interface
* Flat

#### Installation guide
* `git clone https://github.com/Murray1991/PAD-FS.git`
* `cd PAD-FS`
* `mvn clean package`

#### How to run
After the installation phase all the jar executable files will be `target` directory

##### Run a node of the system
* `java -jar target/pad-fs.jar [-b <binding-addr>] [-c <config-file>] [-p <path for the local DB>]`
If the binding address is not provided the program will use the address associated to the hostname of the machine. If the configuration file is not provided the program will check `./pad_fs.config` file if present. If the path is not provided the program will use/create the DB in current directory

##### Run the client
* `java -jar target/pad-fs-cli.jar {-p <key> [<value>] | -g <key> | -r <key> | -l } [-d <dest-addr> | -c <config-file>] [-o <output>]`
The options `-p`, `-g`, `-r` and `-l` respectively refer to the basic operation *put*, *get*, *remove* and *list*. The option `-o` specifies the output file in which to store a get response, if it's not provided the response's value will be printed in the standard output.  If the destination address or the configuration file are not provided, the program will check `./nodes.conf`, a file in which are stored the IP addresses of the PAD-FS nodes.

#### Configuration files
Examples of the configuration files for the client and a server are respectively `nodes.conf` for the client and `pad_fs.conf` for the server, both present in the root directory of the project.

#### Replicated architecture implementation
Passive replication protocol:
* each client can communicate with every node for read/write an item x
* if the client is not the primary server for that item, the client's request is forwarded to the server

In case of node failures or network partitions, the election of the primary server is based on the partitioning of data through consistency hashing. This guarantees high-availability.
