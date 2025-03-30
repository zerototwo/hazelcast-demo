# Hazelcast持久化功能总结

## 1. 持久化概述

Hazelcast持久化功能允许将数据保存到磁盘上，使集群能够在重启后恢复数据。这种功能对于以下场景非常有用：

- 计划内的集群关闭（如维护）
- 非计划的集群关闭（如系统崩溃）
- 单个成员计划重启
- 单个成员意外宕机

### 支持的数据类型

- Map数据结构（IMap）
- JCache数据
- 流处理作业快照
- SQL元数据

### 限制

- 集群在重分区过程中崩溃可能会导致数据丢失
- Map名称不能超过255个字符，且不能包含特殊字符

## 2. 配置选项

### 基本配置

在XML配置文件中启用持久化：

```xml
<persistence enabled="true">
    <base-dir>/var/hazelcast/persistence</base-dir>
    <parallelism>1</parallelism>
    <validation-timeout-seconds>120</validation-timeout-seconds>
    <data-load-timeout-seconds>900</data-load-timeout-seconds>
    <backup-dir>/mnt/backup</backup-dir>
    <auto-remove-stale-data>true</auto-remove-stale-data>
</persistence>
```

### 配置参数

- `base-dir`：指定持久化数据的存储目录
- `parallelism`：控制I/O线程数，影响读写性能
- `validation-timeout-seconds`：验证超时时间
- `data-load-timeout-seconds`：数据加载超时时间
- `backup-dir`：备份目录
- `auto-remove-stale-data`：是否自动移除过期数据

### 集群恢复策略

- `FULL_RECOVERY_ONLY`：默认策略，要求所有成员必须使用其持久化数据
- `PARTIAL_RECOVERY_ALLOWED`：允许部分成员恢复

## 3. 性能特性

### 物理服务器性能

在高性能物理服务器上的测试结果（HP ProLiant，Intel Xeon CPU，768GB内存）：

- 数据大小从10GB到500GB不等
- 对比了HDD（1TB，10K RPM）和SSD性能差异
- SSD媒体在读写操作上显著优于HDD

### AWS云环境性能

在AWS R3实例上的测试结果：

- 使用40百万个键，每个条目1KB
- 总数据大小约38GB
- 测试了三种存储类型：
  - EBS通用SSD（GP2）
  - 带有预置IOPS的EBS（IO1）
  - SSD实例存储

结果显示，SSD实例存储提供最佳性能，特别是在读取操作上。

## 4. 高级功能

### 备份机制

- 可以通过Java API、REST API或Management Center触发备份
- 备份过程是事务性的，确保一致性
- 使用硬链接优化性能，避免重复文件内容

### 恢复机制

#### Force-start（强制启动）

当一个或多个成员无法启动时，可以触发force-start操作，允许集群在删除无法启动成员的持久化存储后继续运行。

#### Partial-start（部分启动）

允许集群在部分成员无法恢复的情况下启动，适用于集群规模较大且容忍部分数据丢失的场景。

### 持久化数据复制

- 支持将持久化数据从一个成员复制到另一个成员
- 便于测试和复现生产环境问题
- 支持不同服务器配置之间的数据迁移

## 5. 存储设计

### 日志结构化存储

- 使用日志结构化方法进行存储
- 所有更新操作都是追加式的，提高写性能
- 更新和删除操作会创建新记录而不是修改旧记录

### 垃圾回收

- 实现并发、增量式、分代垃圾回收
- 通过合并多个较小的Chunk来创建更大的Chunk
- 定期压缩存储以提高读取效率

### I/O优化

- 实现多种I/O最小化方案
- 批处理写操作以减少磁盘访问
- 优化读取路径以提高查询性能

## 6. Intel Optane DC集成

### 注意事项

> Intel已停止支持Intel Optane产品。该支持将在Hazelcast 7.0版本中被移除。

### 配置步骤

1. 使用`ipmctl`和`ndctl`工具配置持久内存
2. 将DIMMs以AppDirect模式运行
3. 创建命名空间并格式化文件系统
4. 配置Hazelcast使用PMem存储目录

### 性能优势

- 通过直接访问持久内存显著提高读写性能
- 适合具有高性能要求的生产环境
- 建议设置`parallelism`为8或12以获得最佳性能 