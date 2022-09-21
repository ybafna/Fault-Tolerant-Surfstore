# Fault-Tolerant-Surfstore
A fault-tolerant Dropbox like cloud storage based on RAFT Protocol

We create a cloud-based file storage service called SurfStore. SurfStore is a networked file storage application that is based on Dropbox, and lets us sync files to and from the “cloud”. We will implement the cloud service, and a client which interacts with your service via gRPC. Multiple clients can concurrently connect to the SurfStore service to access a common, shared set of files. Clients accessing SurfStore “see” a consistent set of updates to files, but SurfStore does not offer any guarantees about operations across files, meaning that it does not support multi-file transactions (such as atomic move).

The SurfStore service is composed of the following two services:
BlockStore 
The content of each file in SurfStore is divided up into chunks, or blocks, each of which has a unique identifier. This service stores these blocks, and when given an identifier, retrieves and returns the appropriate block.
MetaStore
The MetaStore service manages the metadata of files and the entire system. Most importantly, the MetaStore service holds the mapping of filenames to blocks. Furthermore, it should be aware of available BlockStores and map blocks to particular BlockStores.  In a real deployment, a cloud file service like Dropbox or Google Drive will hold exabytes of data, and so will require 10s of thousands of BlockStores or more to hold all that data.

The RAFT paper: https://raft.github.io/raft.pdf
The RAFT website: https://raft.github.io/
