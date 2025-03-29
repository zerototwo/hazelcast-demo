# Hazelcast序列化指南

## 序列化概述

在分布式系统中，数据需要在网络上的集群成员和/或客户端之间传输，这就需要将数据序列化成原始字节。Hazelcast提供了多种序列化选项，您可以根据数据的用途选择最适合的方式。

对于基本数据类型，Hazelcast使用内置序列化器自动处理。但是，对于自定义类或对象，您需要告诉集群如何在网络传输时对它们进行序列化和反序列化。以下内容必须进行序列化：

* 自定义对象
* 执行器服务使用的任务
* 某些条目处理器

## 序列化选项

> **注意：** Portable序列化已被弃用。建议使用Compact序列化，因为Portable序列化将在7.0版本中移除。

Hazelcast提供以下序列化选项：

### 推荐的序列化选项

| 序列化接口 | 优势 | 劣势 | 客户端支持 |
|----------|------|------|----------|
| **Compact序列化** | 为内存使用和效率优化<br>方便、灵活，无需配置<br>无需类实现接口<br>支持模式演变<br>查询和索引时支持部分反序列化 | Hazelcast特有<br>模式不是数据的一部分，但模式分发可能在短期用例中产生成本 | 所有客户端 |
| **HazelcastJsonValue** | 无需服务器端编码 | Hazelcast特有<br>需要在服务器上存储额外元数据 | 所有客户端 |
| **自定义序列化** | 不要求类实现接口<br>方便灵活<br>可基于StreamSerializer或ByteArraySerializer | 必须实现序列化接口<br>需要插件和配置 | 所有客户端 |

### 其他序列化选项

| 序列化接口 | 优势 | 劣势 | 客户端支持 |
|----------|------|------|----------|
| **IdentifiedDataSerializable** | 比Serializable更高效的CPU和内存使用<br>反序列化时不使用反射 | Hazelcast特有<br>必须实现序列化接口<br>必须实现并配置工厂 | 所有客户端 |
| **DataSerializable** | 比Serializable更高效的CPU和内存使用 | Hazelcast特有 | 仅Java |
| **Serializable** | 标准基本Java接口<br>无需实现 | 更多的时间和CPU使用<br>更多的空间占用 | 仅Java |
| **Externalizable** | 标准Java接口<br>比Serializable更高效的CPU和内存使用 | 必须实现序列化接口 | 仅Java |
| **Portable** (已弃用) | 比Serializable更高效的CPU和内存使用<br>反序列化时不使用反射<br>支持版本控制<br>查询时支持部分反序列化 | Hazelcast特有<br>必须实现序列化接口<br>必须实现并配置工厂<br>类定义也与数据一起发送，但每个类只存储一次 | 所有客户端 |

## Hazelcast如何序列化对象

当Hazelcast序列化一个对象时，它按以下顺序检查：

1. 首先检查对象是否为`null`
2. 如果上述检查失败，Hazelcast查找用户指定的CompactSerializer
3. 如果上述检查失败，Hazelcast检查它是否是`com.hazelcast.nio.serialization.DataSerializable`或`com.hazelcast.nio.serialization.IdentifiedDataSerializable`的实例
4. 如果上述检查失败，Hazelcast检查它是否是`com.hazelcast.nio.serialization.Portable`的实例
5. 如果上述检查失败，Hazelcast检查它是否是默认类型之一的实例
6. 如果上述检查失败，Hazelcast查找用户指定的自定义序列化器，即`ByteArraySerializer`或`StreamSerializer`的实现。自定义序列化器是使用输入对象的类及其父类直到Object进行搜索的。如果父类搜索失败，也会检查该类实现的所有接口(不包括`java.io.Serializable`和`java.io.Externalizable`)
7. 如果上述检查失败，Hazelcast检查它是否是`java.io.Serializable`或`java.io.Externalizable`的实例，并且没有使用Java序列化覆盖功能注册全局序列化器
8. 如果上述检查失败，Hazelcast使用已注册的全局序列化器(如果存在)
9. 如果上述检查失败，Hazelcast尝试从对象的类自动提取模式(如果可能)

如果所有上述检查都失败，序列化将失败。当一个类实现多个接口时，上述步骤对于确定Hazelcast使用的序列化机制非常重要。

## 内置序列化的数据类型

默认情况下，Hazelcast为以下数据类型优化了序列化。您不需要自己序列化或反序列化这些类型：

* `Class`, `Optional`, `Date`, `BigInteger`, `BigDecimal`, `ArrayList`, `LinkedList`, `CopyOnWriteArrayList/Set`, `HashMap/Set`, `ConcurrentSkipListMap/Set`, `ConcurrentHashMap`, `LinkedHashMap/Set`, `TreeMap/Set`, `ArrayDeque`, `LinkedBlockingQueue`, `ArrayBlockingQueue`, `PriorityBlockingQueue`, `PriorityQueue`, `DelayQueue`, `SynchronousQueue`, `LinkedTransferQueue`

如果您希望为这些类型实现自己的序列化，可以配置Hazelcast以启用覆盖默认序列化器的功能。

## 序列化实现示例

### Compact序列化示例

```java
// 定义实体类
public class Employee {
    private int id;
    private String name;
    private String department;
    
    // 构造函数、getter和setter方法
    // ...
}

// 定义序列化器
public class EmployeeSerializer implements CompactSerializer<Employee> {
    @Override
    public Employee read(CompactReader reader) {
        Employee employee = new Employee();
        employee.setId(reader.readInt("id"));
        employee.setName(reader.readString("name"));
        employee.setDepartment(reader.readString("department"));
        return employee;
    }
    
    @Override
    public void write(CompactWriter writer, Employee employee) {
        writer.writeInt("id", employee.getId());
        writer.writeString("name", employee.getName());
        writer.writeString("department", employee.getDepartment());
    }
    
    @Override
    public String getTypeName() {
        return "employee";
    }
    
    @Override
    public Class<Employee> getCompactClass() {
        return Employee.class;
    }
}

// 注册序列化器
SerializationConfig serConfig = new SerializationConfig();
serConfig.getCompactSerializationConfig().addSerializer(new EmployeeSerializer());
Config config = new Config();
config.setSerializationConfig(serConfig);
```

### HazelcastJsonValue示例

```java
// 直接使用JSON字符串
String json = "{\"id\":1,\"name\":\"John Doe\",\"department\":\"IT\"}";
HazelcastJsonValue jsonValue = new HazelcastJsonValue(json);
map.put("employee1", jsonValue);

// 从对象转换
Employee employee = new Employee(2, "Jane Smith", "HR");
ObjectMapper mapper = new ObjectMapper();
String employeeJson = mapper.writeValueAsString(employee);
HazelcastJsonValue jsonValue = new HazelcastJsonValue(employeeJson);
map.put("employee2", jsonValue);
```

### 自定义序列化示例

```java
// 实现StreamSerializer
public class EmployeeStreamSerializer implements StreamSerializer<Employee> {
    @Override
    public void write(ObjectDataOutput out, Employee employee) throws IOException {
        out.writeInt(employee.getId());
        out.writeString(employee.getName());
        out.writeString(employee.getDepartment());
    }
    
    @Override
    public Employee read(ObjectDataInput in) throws IOException {
        int id = in.readInt();
        String name = in.readString();
        String department = in.readString();
        return new Employee(id, name, department);
    }
    
    @Override
    public int getTypeId() {
        return 1000; // 自定义类型ID，必须唯一
    }
}

// 注册序列化器
SerializationConfig serConfig = new SerializationConfig();
serConfig.addSerializerConfig(
    new SerializerConfig()
        .setImplementation(new EmployeeStreamSerializer())
        .setTypeClass(Employee.class));
Config config = new Config();
config.setSerializationConfig(serConfig);
```

## 序列化最佳实践

1. **选择合适的序列化方法**：
   - 对于新项目，优先考虑使用Compact序列化
   - 如果只需要简单地在客户端和服务器之间传输JSON，使用HazelcastJsonValue
   - 当需要高度自定义序列化过程时，使用自定义序列化

2. **避免序列化过大的对象**：大对象可能导致性能问题和内存压力

3. **确保序列化一致性**：序列化和反序列化必须在所有集群成员和客户端上保持一致

4. **版本控制**：考虑对象结构的演变，使用支持版本控制的序列化方法(如Compact序列化)

5. **性能考虑**：
   - 监控序列化和反序列化的性能
   - 在性能敏感的场景中避免使用Java Serializable

6. **安全性**：避免序列化不受信任的数据，这可能导致安全漏洞

## 序列化配置示例

```java
Config config = new Config();
SerializationConfig serializationConfig = config.getSerializationConfig();

// 启用紧凑序列化
serializationConfig.getCompactSerializationConfig()
    .setEnabled(true)
    .addSerializer(new EmployeeSerializer());

// 配置全局序列化器
serializationConfig.setGlobalSerializerConfig(
    new GlobalSerializerConfig()
        .setImplementation(new MyGlobalSerializer())
        .setOverrideJavaSerialization(true));

// 禁用对Java序列化的支持（增强安全性）
serializationConfig.setAllowUnsafe(false);
serializationConfig.setEnableCompression(true);
serializationConfig.setEnableSharedObject(true);
```

## 注意事项和常见问题

1. **类路径问题**：确保所有序列化类在集群中所有成员和客户端的类路径上可用

2. **类版本兼容**：在滚动升级期间，确保新旧版本的类能够互相理解

3. **序列化ID冲突**：确保自定义序列化器的类型ID不会冲突

4. **性能调优**：根据应用需求在序列化速度和大小之间找到平衡

5. **序列化异常**：处理序列化失败的情况，提供清晰的错误消息和恢复策略

## 参考资料

- [Hazelcast序列化文档](https://docs.hazelcast.com/hazelcast/latest/serialization/serialization)
- [Compact序列化详细说明](https://docs.hazelcast.com/hazelcast/latest/serialization/compact-serialization)
- [HazelcastJsonValue](https://docs.hazelcast.com/hazelcast/latest/serialization/serializing-json)
- [自定义序列化](https://docs.hazelcast.com/hazelcast/latest/serialization/custom-serialization) 