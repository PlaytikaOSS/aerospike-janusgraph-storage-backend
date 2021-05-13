[![CircleCI](https://circleci.com/gh/Playtika/aerospike-janusgraph-storage-backend.svg?style=shield&circle-token=2abe3b0bc221b8c608f5d1f825f621e4a5ea6902)](https://circleci.com/gh/Playtika/aerospike-janusgraph-storage-backend/tree/develop)
[![codecov](https://codecov.io/gh/Playtika/aerospike-janusgraph-storage-backend/branch/develop/graph/badge.svg)](https://codecov.io/gh/Playtika/aerospike-janusgraph-storage-backend)
[![Codacy Badge](https://api.codacy.com/project/badge/Grade/76d508c67fc04544bc7270140ca8be26)](https://www.codacy.com/app/PlaytikaCodacy/aerospike-janusgraph-storage-backend?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=Playtika/aerospike-janusgraph-storage-backend&amp;utm_campaign=Badge_Grade)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.playtika.janusgraph/aerospike-storage-backend/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.playtika.janusgraph/aerospike-storage-backend)

# Aerospike storage backend for Janusgraph

## Overview

Aerospike based implementation of Janusgraph storage backend. 
When to use: If you need horizontally scalable graph DB backed by Aerospike.

## Key features

### Emulate transactions via WAL
The main difference with other traditional backends (Canssandra, Berkeley) is that Aerospike does not support transactions.
On each commit Jansugraph writes batch of updates that should be applied to storage backend all together. 
In other case graph may become inconsistent.   
So we need to emulate transactional behaviour and not surprisingly made it via Write Ahead Log. 
We use [No Sql Batch Updater](https://github.com/Playtika/nosql-batch-updater) library to achieve this.
Prior to applying updates we save all batch as one record in Aerospike separate namespace and remove this record after all updates being applied.
This allows [WriteAheadLogCompleter](https://github.com/Playtika/nosql-batch-updater/blob/master/batch-updater/src/main/java/nosql/batch/update/wal/WriteAheadLogCompleter.java) 
that runs on each node in separate thread to finish (with configured delay) all needed updates in case of some node had died in the middle of the batch.

### Deferred locking
Collects all locks that transaction needs and acquire them just in commit phase. 
Allows us to run all lock acquisitions in parallel. This approach caused Aerospike storage backend to be classified
as _optimisticLocking_ In terms of Janusgraph DB.

## Known limitations

### Record size 
Janusgraph keeps vertex and all adjacent edges in one record.
 That makes it sensitive to max record value size in key-value storage.   
 Aerospike record size is limited by 1Mb by default and can be increased up to 8Mb in
  namespace configuration. It makes sens to configure WAL namespace to use maximum value (8Mb).

### Dirty reads
While emulating eventually consistent batch updates it is still possible to have dirty reads that may lead to some unwanted side effect
like ghost vertices. You should try to avoid concurrent deletion and update of the same vertex. 
The best option is to use some external synchronization while doing such thing.       

## How to run
### Embedded Mode
In our microservice architecture we run Janusgraph in embedded mode. 
This mode uses Janusgraph and Aerospike storage backend just as library to correctly access and persist graphs in Aerospike.   

It allows our services to:
 - communicate with Janusgraph in the same JVM with minimal overheads
 - scale up/down Janusgraph together with the service  

#### Steps to introduce 
* Add dependency to Aerospike storage backend to your project  
```
<dependency>
    <groupId>com.playtika.janusgraph</groupId>
    <artifactId>aerospike-storage-backend</artifactId>
</dependency>
```
* Instantiate JanusGraph
```
ModifiableConfiguration config = buildGraphConfiguration();
config.set(STORAGE_HOSTS, new String[]{aerospikeHost}); //Aerospike host
config.set(STORAGE_PORT, container.getMappedPort(aerospikePort));
config.set(STORAGE_BACKEND, "com.playtika.janusgraph.aerospike.AerospikeStoreManager");
config.set(NAMESPACE, aerospikeNamespace);
config.set(WAL_NAMESPACE, walNamespace);  //Aspike namespace to use for Write Ahead Log
config.set(GRAPH_PREFIX, "test");  //used as prefix for Aspike sets. Allows to run several graphs in one Aspike namespace  
//!!! need to prevent small batches mutations as we use deferred locking approach !!!
config.set(BUFFER_SIZE, AEROSPIKE_BUFFER_SIZE);
config.set(TEST_ENVIRONMENT, true); //# whether we should use durable deletes (not available in community version of Aspike) 
config.set(ConfigOptions.SCAN_PARALLELISM, 1);  //allow tu run scans in single thread only 

JanusGraph graph = JanusGraphFactory.open(config);
```
* Run Gremlin queries
```
graph.traversal().V().has("name", "jupiter")
```

### Server Mode
  
## Benchmark

| Benchmark | Mode  |  Cnt  | Score  | Error  | Units |
|:---       |   :-: |   :-: |   :-:  |   :-:  |  :-:  |
|aerospike | thrpt |  30 | 0.106 | ± 0.004 | ops/s |
|cassandra | thrpt |  30 | 0.008 | ± 0.001 | ops/s |

This benchmark was run using standard 'cassandra:3.11' docker image and custom aerospike image that doesn't keep any data in memory.
https://github.com/kptfh/aerospike-server.docker

To run benchmarks and test on your local machine you just need to have docker installed.
