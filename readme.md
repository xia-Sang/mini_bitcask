# mini_bitcask: A Starting Point for Distributed Data Storage
[中文版](./readme-zh.md)
> For personal learning and exploration  
`mini_bitcask` is a simplified implementation of the Bitcask key-value storage model, focusing on efficient data storage and retrieval in single-node environments. This project serves as a foundation for deeper research into distributed storage systems, making it an excellent starting point for learning and practical experimentation.

## Current Objectives

1. **Understand Core Storage Principles**: Implementing a simplified key-value storage model to understand the collaboration between file storage and in-memory indexing structures.
2. **Data Compression and Space Optimization**: Using the `Flush` functionality to explore how to minimize wasted storage space, providing insights into garbage collection mechanisms in distributed systems.
3. **Single-Node Model Validation**: Providing a functional local storage component as a basis for building distributed systems.

## Future Research Plan

### 1. From Single-Node to Distributed Systems
- **Data Sharding**: Distribute data across multiple storage nodes to enhance storage capacity and throughput.
- **Replication Management**: Implement multi-replica mechanisms to ensure high availability and fault tolerance.
- **Consistency Protocols**: Introduce protocols such as Raft or Paxos to achieve data consistency across nodes.

### 2. Core Features of Distributed Storage
- **Distributed Indexing**: Develop distributed hash tables (DHT) or skip list structures to support fast global key queries.
- **Load Balancing**: Distribute workloads across nodes to optimize system performance.
- **Disaster Recovery**: Design backup and recovery strategies across nodes to improve reliability.

### 3. Performance Optimization
- **Efficient Logging**: Optimize log-writing performance to support high-concurrency operations.
- **Intelligent Caching**: Add a caching layer to reduce reliance on disk I/O.
- **Compression and Encryption**: Introduce data compression and encryption mechanisms to enhance storage efficiency and security.

### 4. Scalability and Manageability
- **Dynamic Scaling**: Support the dynamic addition and removal of nodes to ensure seamless scalability.
- **Monitoring and Tooling**: Develop monitoring and debugging tools for distributed storage systems to simplify operations.

## Long-Term Vision

The ultimate goal of `mini_bitcask` is to provide a complete learning pathway from single-node storage to distributed storage, culminating in a modern distributed storage engine with the following characteristics:
1. **Strong Consistency**: Ensures data consistency across nodes through robust consensus protocols.
2. **High Availability**: Provides fault-tolerant mechanisms to ensure services remain operational during node failures.
3. **High Performance**: Efficiently handles massive data volumes and high-concurrency access.

By progressively improving `mini_bitcask`, the project aspires to build a distributed storage engine capable of meeting complex real-world requirements in distributed environments.