# PAD-FS: project guidelines and design choices

#### Overview
* A simple but distributed key-value storage system
* Simple API: put, get, list, remove
* REST or CLI interface
* Flat

#### CAP Theorem
![alt text](http://www.developer.com/imagesvr_ce/4400/Mongo1.png "CAP Theorem")

#### Consistency Model
* The goal is to guarantee Availability and Partition Tolerance (AP)
* The consistency model chosen is **Eventual Consistency**

#### How data are partitioned and replicated among the PAD-FS nodes?
* Consistent Hashing with virtual nodes

#### How consistency among replicas is achieved?
* Anti-entropy protocol for replica synchronization
* Versioning with vector clocks facilitate consistency and conflict resolution (last write wins)

#### Membership and failure detection
* Gossip-based distributed protocol

#### How temporary/permanent failures are handled?
* With the same anti-entropy protocol for replica synchronization

#### Replicated architecture implementation
Passive replication protocol: 
* each client can communicate with every node for read/write an item x
* if the client is not the primary server for that item, the client's request is forwarded to the server

In case of node failures or network partitions, the election of the primary server is based on the partitioning of data through consistency hashing. This guarantees high-availability.





