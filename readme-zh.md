# mini_bitcask：分布式数据存储的起点
>个人学习使用


`mini_bitcask` 是一个简化版的 Bitcask 键值存储模型的实现，重点在于单机环境下的高效数据存储和检索操作。作为一个项目，它为深入研究分布式存储系统提供了基础，是分布式数据存储学习和实践的良好开端。

## 当前实现目标

1. **理解数据存储的核心原理**：通过实现简化的键值存储模型，掌握文件存储和基于内存的索引结构的协作机制。
2. **数据压缩与空间优化**：通过 `Flush` 功能探索如何减少存储空间的浪费，为分布式存储中垃圾回收等机制打下基础。
3. **单机模型验证**：为分布式系统的构建提供可运行的本地存储组件。

## 下一步研究计划

### 1. 从单机到分布式
- **数据分片**：将数据按键分布到多个存储节点（Sharding），以提升存储容量和吞吐量。
- **副本管理**：实现多副本机制，确保数据的高可用性和容错性。
- **一致性协议**：引入 Raft 或 Paxos 协议，实现节点间数据一致性。

### 2. 分布式存储基础功能
- **分布式索引**：开发分布式哈希表（DHT）或跳表结构，支持快速的全局键查询。
- **负载均衡**：在多个节点间分散负载，优化系统性能。
- **容灾与恢复**：设计跨节点的数据备份与恢复策略，提升系统可靠性。

### 3. 深入优化性能
- **高效日志结构**：优化日志写入性能，支持高并发操作。
- **智能缓存**：在存储层上增加缓存机制，减少对磁盘 I/O 的依赖。
- **压缩与加密**：引入数据压缩和加密机制，提升存储效率和安全性。

### 4. 增强可扩展性与管理
- **动态扩容与缩容**：支持节点的动态加入和退出，确保系统无缝扩展。
- **监控与运维工具**：开发分布式存储的监控与调试工具，简化运维。

## 长远目标

`mini_bitcask` 的最终目标是成为一个从单机存储到分布式存储的完整学习路径，实现一个具有以下特性的分布式存储引擎：
1. **强一致性**：通过一致性协议保证多节点间的数据一致。
2. **高可用性**：提供副本容灾机制，确保服务在节点故障时仍可用。
3. **高性能**：支持海量数据存储与高并发访问。

通过逐步完善 `mini_bitcask`，最终构建一个能够处理分布式场景复杂需求的现代分布式存储引擎。