[![CircleCI](https://circleci.com/gh/Playtika/aerospike-janusgraph-storage-backend.svg?style=shield&circle-token=2abe3b0bc221b8c608f5d1f825f621e4a5ea6902)](https://circleci.com/gh/Playtika/aerospike-janusgraph-storage-backend/tree/develop)
[![codecov](https://codecov.io/gh/Playtika/aerospike-janusgraph-storage-backend/branch/develop/graph/badge.svg)](https://codecov.io/gh/Playtika/aerospike-janusgraph-storage-backend)
[![Codacy Badge](https://api.codacy.com/project/badge/Grade/76d508c67fc04544bc7270140ca8be26)](https://www.codacy.com/app/PlaytikaCodacy/aerospike-janusgraph-storage-backend?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=Playtika/aerospike-janusgraph-storage-backend&amp;utm_campaign=Badge_Grade)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.playtika.janusgraph/aerospike-storage-backend/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.playtika.janusgraph/aerospike-storage-backend)

# Aerospike storage backend for Janusgraph

| Benchmark | Mode  |  Cnt  | Score  | Error  | Units |
|:---       |   :-: |   :-: |   :-:  |   :-:  |  :-:  |
|aerospike | thrpt |  30 | 0.106 | ± 0.004 | ops/s
|cassandra | thrpt |  30 | 0.009 | ± 0.001 | ops/s

This benchmark was run using standard 'cassandra:3.11' docker image and custom aerospike image that doesn't keep any data in memory.
https://github.com/kptfh/aerospike-server.docker

To run benchmarks and test on your local machine you just need to have docker installed.
