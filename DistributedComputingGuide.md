# Hazelcast分布式计算指南

## 什么是分布式计算？

分布式计算是在不同的集群成员上运行计算任务的过程。通过分布式计算，计算速度更快，这得益于以下优势：

* 利用集群的组合处理能力
* 通过在拥有数据的集群上运行计算来减少网络跳转

## 可用工具

Hazelcast根据您的使用场景提供以下分布式计算工具：

* **条目处理器（Entry Processor）**：在集群成员（服务器）上更新、删除和读取映射条目
* **执行器服务（Executor Service）**：在集群成员上执行您自己的Java代码并获取结果
* **管道（Pipeline）**：创建在集群成员上运行快速批处理或流处理的数据管道

## 何时使用条目处理器

如果您对映射执行批量处理，条目处理器是一个很好的选择。通常，您执行一个键循环，执行`map.get(key)`，修改值，最后使用`map.put(key,value)`将条目放回映射。如果您从不存在键的客户端或成员执行此过程，则每次更新实际上会执行两次网络跳转：第一次检索数据，第二次更新修改后的值。

如果您正在执行上述过程，应考虑使用条目处理器。条目处理器在数据所在的成员上执行读取和更新操作，从而消除了昂贵的网络跳转。

> **重要提示**：条目处理器旨在每次调用处理单个条目。在条目处理器中处理多个条目和数据结构不受支持，因为可能会导致死锁。要一次处理多个条目，请使用管道。

## 何时使用执行器服务

当您想要在集群成员上运行任意Java代码时，执行器服务是理想选择。执行器服务特别适合：

* 需要在特定成员上执行的复杂计算
* 需要将任务分发到集群的场景
* 长时间运行的批处理作业

## 何时使用管道

当您想要执行涉及多个条目（聚合、连接等）的处理，或涉及需要并行执行的多个计算步骤时，管道是个不错的选择。管道允许您使用条目处理器接收器，根据计算结果更新映射。

管道特别适合：
* 数据流处理
* ETL操作
* 复杂的数据转换
* 实时分析

## 分布式计算的实现方式

### 条目处理器

条目处理器可以在不将条目移出集群成员的情况下更新它们，从而减少网络开销和提高性能。它直接在存储数据的集群成员上执行，并可以原子方式访问和修改条目。

**示例：**
```java
// 定义条目处理器
EntryProcessor<String, Integer, Integer> incrementor = 
    (entry) -> {
        Integer value = entry.getValue();
        entry.setValue(value + 1);
        return value;
    };

// 在单个条目上执行
Integer oldValue = map.executeOnKey("key1", incrementor);

// 在多个条目上批量执行
Map<String, Integer> results = map.executeOnKeys(
    Set.of("key1", "key2", "key3"), incrementor);
```

### 执行器服务

Hazelcast的执行器服务在集群范围内分发和执行任务，支持负载均衡和故障转移机制。

**Java执行器服务**是最基本的实现，提供标准的Java `ExecutorService` API，但具有分布式特性。

**示例：**
```java
IExecutorService executor = hazelcastInstance.getExecutorService("my-executor");

// 提交任务到集群
Future<String> future = executor.submit(new MyCallable());
String result = future.get();

// 将任务定向到特定成员
Member member = ...;
Future<String> future = executor.submitToMember(new MyCallable(), member);

// 将任务定向到特定键（共存原则）
Future<String> future = executor.submitToKeyOwner(new MyCallable(), "targetKey");
```

**耐久执行器服务**提供任务持久化以及重新平衡和节点故障后的自动恢复功能。

**调度执行器服务**支持按计划或定期执行任务，类似于标准Java的`ScheduledExecutorService`，但在分布式环境中。

### 管道

管道是Hazelcast的高级数据处理API，使创建连续处理步骤变得简单，非常适合ETL操作、流处理和复杂聚合。

**示例：**
```java
// 创建管道
Pipeline pipeline = Pipeline.create();

// 从源读取数据
BatchSource<String> source = Sources.list("my-list");

// 处理数据
BatchStage<String> transformed = pipeline
    .readFrom(source)
    .map(item -> item.toUpperCase())
    .filter(item -> !item.isEmpty());

// 写入接收器
transformed.writeTo(Sinks.map("result-map"));

// 提交作业
Job job = hazelcastInstance.getJet().newJob(pipeline);
```

## 数据摄取与分布式计算

将数据导入到Hazelcast中是分布式计算的重要一环。Hazelcast提供两种主要的数据摄取方式：

### 1. Pipeline数据摄取

Pipeline不仅是处理数据的工具，也是摄取数据的强大方式：

```java
// 从JDBC数据源读取数据
Pipeline pipeline = Pipeline.create();
pipeline.readFrom(Sources.jdbc("SELECT * FROM products", 
                            dataSource::getConnection))
      .map(row -> new Product(row.getString("name"), 
                             row.getDouble("price"), 
                             row.getString("category")))
      .writeTo(Sinks.map("product-data"));
```

Hazelcast内置了多种连接器，包括：
- Apache Kafka
- Amazon Kinesis
- 各种云存储服务(S3, Azure Blob, GCS)
- 文件系统
- JDBC数据源

### 2. MapStore数据摄取

与EntryProcessor结合使用的理想选择：

```java
// 配置MapStore
MapStoreConfig mapStoreConfig = new MapStoreConfig()
    .setClassName("org.example.hazelcast.demo.MyMapStore")
    .setEnabled(true);

Config config = new Config();
config.getMapConfig("my-map")
    .setMapStoreConfig(mapStoreConfig);
```

实现MapStore接口：
```java
public class MyMapStore implements MapStore<String, Employee> {
    @Override
    public void store(String key, Employee value) {
        // 写入数据库
    }
    
    @Override
    public Employee load(String key) {
        // 从数据库加载
    }
    
    // 其他方法实现...
}
```

## 选择合适的工具

| 特性 | 条目处理器 | 执行器服务 | 管道 |
|-----|----------|----------|------|
| 适用场景 | 简单的条目更新 | 任意Java代码执行 | 复杂数据流和ETL |
| 操作原子性 | 是 | 否 | 否 |
| 执行位置 | 数据所在节点 | 可配置 | 可优化分布 |
| 结果处理 | 直接返回 | 异步Future | 流式处理 |
| 扩展性 | 中等 | 高 | 非常高 |
| 数据摄取集成 | MapStore | 自定义 | 内置连接器 |

## 性能考虑

- **条目处理器**：适用于简单的更新操作，特别是当您需要更新大量条目但每个更新逻辑相对简单时。
- **执行器服务**：适用于独立任务，尤其是那些不依赖于大量数据传输的任务。
- **管道**：适用于复杂的数据处理场景，尤其是涉及大量数据的流处理或批处理场景。

## 最佳实践

1. 对于单个条目或少量条目的简单更新，使用条目处理器
2. 对于需要在特定集群成员上运行的复杂逻辑，使用执行器服务
3. 对于数据流处理、ETL操作或复杂聚合，使用管道
4. 避免在条目处理器中处理多个条目或执行耗时操作
5. 在执行器服务中实现适当的超时和故障处理
6. 对于复杂的管道作业，利用检查点机制进行故障恢复
7. 使用User Code Namespaces组织和管理分布式计算代码

## 使用User Code Namespaces

User Code Namespaces是Hazelcast提供的机制，用于管理Java类路径资源并确保不同命名空间间的资源访问隔离性。对于分布式计算尤其重要：

```java
// 静态配置示例
Config config = new Config();
UserCodeNamespaceConfig namespaceConfig = 
    new UserCodeNamespaceConfig("executor-namespace")
        .addURL("file:///path/to/your/tasks.jar");
config.addUserCodeNamespaceConfig(namespaceConfig);

// 关联到ExecutorService
config.getExecutorConfig("demo-executor")
    .setUserCodeNamespace("executor-namespace");
```

## 注意事项

- 在条目处理器和执行器任务中实现的业务逻辑应该是无状态的
- 所有用于处理的类必须是可序列化的
- 避免在分布式任务中引用外部数据，应该通过参数传递所需数据
- 对于长时间运行的操作，最好使用管道而不是条目处理器或执行器服务

通过选择适合您具体使用场景的正确分布式计算工具，您可以显著提高应用程序的性能和可扩展性，同时减少网络开销。

## 参考资料

- [Hazelcast文档 - 用户代码命名空间](https://docs.hazelcast.com/hazelcast/latest/clusters/user-code-namespaces)
- [Hazelcast文档 - 数据摄取概述](https://docs.hazelcast.com/hazelcast/latest/ingest/overview) 