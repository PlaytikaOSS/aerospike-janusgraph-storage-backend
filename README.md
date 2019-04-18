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
Prior to applying updates we save all batch as one record in Aerospike separate namespace and remove this record after all updates being applied.
This allows [WriteAheadLogCompleter](https://github.com/Playtika/aerospike-janusgraph-storage-backend/blob/develop/aerospike-storage-backend/src/main/java/com/playtika/janusgraph/aerospike/wal/WriteAheadLogCompleter.java) 
that runs on each node in separate thread to finish (with configured delay) all needed updates in case of some node had died in the middle of the batch.

### Deferred locking
Collects all locks that transaction needs and acquire them just in commit phase. 
Allows us to run all lock acquisitions in parallel. This approach caused Aerospike storage backend to be classified
as _optimisticLocking_ In terms of Janusgraph DB.

## Known limitations
Janusgraph keeps vertex and all adjacent edges in one record.
 That makes it sensitive to max record value size in key-value storage.   
 Aerospike record size is limited by 1Mb by default and can be increased up to 8Mb in
  namespace configuration. It makes sens to configure WAL namespace to use maximum value (8Mb).          
      

## Benchmark

| Benchmark | Mode  |  Cnt  | Score  | Error  | Units |
|:---       |   :-: |   :-: |   :-:  |   :-:  |  :-:  |
|aerospike | thrpt |  30 | 0.106 | ± 0.004 | ops/s |
|cassandra | thrpt |  30 | 0.009 | ± 0.001 | ops/s |

This benchmark was run using standard 'cassandra:3.11' docker image and custom aerospike image that doesn't keep any data in memory.
https://github.com/kptfh/aerospike-server.docker

To run benchmarks and test on your local machine you just need to have docker installed.
