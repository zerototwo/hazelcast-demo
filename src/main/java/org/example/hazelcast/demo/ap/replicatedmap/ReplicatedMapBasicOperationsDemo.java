package org.example.hazelcast.demo.ap.replicatedmap;

import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.MapEvent;
import com.hazelcast.replicatedmap.ReplicatedMap;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Hazelcast ReplicatedMap 基本操作示例
 * 
 * 官方定义：
 * Hazelcast ReplicatedMap是一种特殊的分布式Map实现，它复制而不是分区数据。它提供了最终一致性保证，
 * 即在某些情况下，其行为类似于NoSQL解决方案。
 * 
 * 主要特性：
 * 1. 数据复制：每个集群成员存储数据的完整副本，而不是分区
 * 2. 最终一致性：所有的写操作会异步复制到所有集群成员
 * 3. 快速读取：读取总是从本地副本获取，不需要网络调用
 * 4. 无分区感知：不像分区Map那样有数据所有权概念
 * 5. 弱一致性：更新会在后台传播，有短暂的不一致窗口
 * 6. 高可用性：由于每个节点存储完整数据，节点故障不影响读可用性
 * 7. 较低的写吞吐量：相比分区Map，写入性能较低
 * 
 * 适用场景：
 * - 读多写少的应用场景
 * - 对数据一致性要求不高的场景
 * - 需要低延迟本地读取的场景
 * - 配置和参考数据存储
 * - 需要更接近NoSQL行为的应用
 * 
 * 不适用场景：
 * - 大数据量存储（因为每个节点存储全量数据）
 * - 写密集型应用
 * - 需要强一致性保证的场景
 * 
 * ReplicatedMap与其他Map的区别：
 * - 与IMap(常规Map)区别：IMap分区数据并提供强一致性，ReplicatedMap复制数据并提供最终一致性
 * - 与MultiMap区别：MultiMap允许一个键多个值，而ReplicatedMap仍是键值一对一
 * - 与NearCache区别：NearCache是IMap的本地缓存，而ReplicatedMap在每个节点有完整副本
 * 
 * 本示例演示了Hazelcast ReplicatedMap的基本操作，包括创建、
 * 添加、检索、删除和监听等功能以及其最终一致性特性。
 */
@Component
public class ReplicatedMapBasicOperationsDemo {

  private final HazelcastInstance hazelcastInstance;

  public ReplicatedMapBasicOperationsDemo(HazelcastInstance hazelcastInstance) {
    this.hazelcastInstance = hazelcastInstance;
  }

  public void runAllExamples() {
    System.out.println("\n开始ReplicatedMap示例...");
    basicOperationsExample();
    inMemoryFormatExample();
    entryListenerExample();
    concurrentAccessExample();
    System.out.println("\nReplicatedMap示例完成！");
  }

  /**
   * ReplicatedMap基本操作示例
   */
  public void basicOperationsExample() {
    System.out.println("\n=== ReplicatedMap基本操作示例 ===");

    // 获取ReplicatedMap实例
    ReplicatedMap<String, String> repMap = hazelcastInstance.getReplicatedMap("basic-replicated-map-demo");

    try {
      // 清空ReplicatedMap，确保演示从空开始
      repMap.clear();

      System.out.println("添加键值对到ReplicatedMap");

      // 向ReplicatedMap添加值
      repMap.put("key1", "value1");
      repMap.put("key2", "value2");
      repMap.put("key3", "value3");

      System.out.println("ReplicatedMap大小: " + repMap.size());

      // 获取值
      String value = repMap.get("key1");
      System.out.println("\n'key1'对应的值: " + value);

      // 检查键是否存在
      boolean containsKey = repMap.containsKey("key2");
      System.out.println("是否包含键 'key2': " + containsKey);

      // 检查值是否存在
      boolean containsValue = repMap.containsValue("value3");
      System.out.println("是否包含值 'value3': " + containsValue);

      // 获取所有键值对
      Set<Map.Entry<String, String>> entries = repMap.entrySet();
      System.out.println("\n所有键值对:");
      for (Map.Entry<String, String> entry : entries) {
        System.out.println(entry.getKey() + " -> " + entry.getValue());
      }

      // 获取所有键
      Set<String> keys = repMap.keySet();
      System.out.println("\n所有键: " + keys);

      // 修改值
      repMap.put("key1", "updated-value1");
      System.out.println("\n修改后'key1'对应的值: " + repMap.get("key1"));

      // 删除条目
      String removed = repMap.remove("key2");
      System.out.println("移除'key2'键，返回值: " + removed);
      System.out.println("移除后ReplicatedMap大小: " + repMap.size());

      // 批量添加
      Map<String, String> batch = Map.of(
          "batch-key1", "batch-value1",
          "batch-key2", "batch-value2");
      repMap.putAll(batch);
      System.out.println("\n批量添加后ReplicatedMap大小: " + repMap.size());

    } finally {
      // 清理资源
      repMap.destroy();
    }
  }

  /**
   * 演示ReplicatedMap的内存格式选项
   */
  public void inMemoryFormatExample() {
    System.out.println("\n=== ReplicatedMap内存格式示例 ===");

    ReplicatedMap<String, Person> repMap = hazelcastInstance.getReplicatedMap("memory-format-demo");

    try {
      repMap.clear();

      System.out.println("ReplicatedMap支持两种内存格式:");
      System.out.println("1. OBJECT (默认): 数据以反序列化形式存储");
      System.out.println("2. BINARY: 数据以序列化二进制格式存储，每次请求都需要反序列化");

      System.out.println("\n演示使用对象格式:");

      // 添加Person对象
      Person person1 = new Person("张三", 30);
      repMap.put("person1", person1);

      // 获取对象（不需要反序列化，因为它已经是对象形式）
      Person retrievedPerson = repMap.get("person1");
      System.out.println("获取到的Person对象: " + retrievedPerson);

      System.out.println("\n注意：在OBJECT格式中，直接修改对象不会反映到其他成员节点");
      System.out.println("除非使用put方法将修改后的对象重新放入map");

      System.out.println("\n在BINARY格式中，对象总是序列化存储，这提供了更高的封装性");
      System.out.println("XML配置示例:");
      System.out.println("<replicatedmap name=\"default\">");
      System.out.println("    <in-memory-format>BINARY</in-memory-format>");
      System.out.println("</replicatedmap>");

    } finally {
      repMap.destroy();
    }
  }

  /**
   * 演示ReplicatedMap的条目监听器
   */
  public void entryListenerExample() {
    System.out.println("\n=== ReplicatedMap条目监听器示例 ===");

    ReplicatedMap<String, String> repMap = hazelcastInstance.getReplicatedMap("listener-demo");

    try {
      repMap.clear();

      // 添加监听器
      System.out.println("添加条目监听器到ReplicatedMap");
      repMap.addEntryListener(new DemoEntryListener());

      System.out.println("\n执行操作触发监听器事件:");

      // 添加条目
      repMap.put("event-key1", "event-value1");

      // 更新条目
      repMap.put("event-key1", "event-value1-updated");

      // 删除条目
      repMap.remove("event-key1");

      System.out.println("\n注意: 对于ReplicatedMap，EntryListener事件只反映本地数据的变化");
      System.out.println("因为复制是异步的，事件可能在不同成员上的不同时间触发");

      System.out.println("\nXML监听器配置示例:");
      System.out.println("<replicatedmap name=\"default\">");
      System.out.println("    <entry-listeners>");
      System.out.println("        <entry-listener include-value=\"true\">com.example.MyEntryListener</entry-listener>");
      System.out.println("    </entry-listeners>");
      System.out.println("</replicatedmap>");

    } finally {
      repMap.destroy();
    }
  }

  /**
   * 演示ReplicatedMap的并发访问特性
   */
  public void concurrentAccessExample() {
    System.out.println("\n=== ReplicatedMap并发访问示例 ===");

    ReplicatedMap<String, String> repMap = hazelcastInstance.getReplicatedMap("concurrent-access-demo");

    try {
      repMap.clear();

      System.out.println("ReplicatedMap的主要特点是在集群的所有成员上复制每个条目");
      System.out.println("这提供了快速的读取性能，因为数据总是本地可用");

      // 填充一些数据
      for (int i = 1; i <= 3; i++) {
        repMap.put("concurrent-key" + i, "concurrent-value" + i);
      }

      System.out.println("\n当新成员加入集群时，ReplicatedMap支持两种填充模式:");
      System.out.println("1. 异步填充 (默认): 不阻塞读写操作，但可能返回null值");
      System.out.println("2. 同步填充: 阻塞所有访问，直到填充操作完成");

      System.out.println("\nXML填充模式配置示例:");
      System.out.println("<replicatedmap name=\"default\">");
      System.out.println("    <async-fillup>true</async-fillup> <!-- true: 异步, false: 同步 -->");
      System.out.println("</replicatedmap>");

    } finally {
      repMap.destroy();
    }
  }

  /**
   * 演示用的Person类
   */
  public static class Person {
    private String name;
    private int age;

    public Person(String name, int age) {
      this.name = name;
      this.age = age;
    }

    public String getName() {
      return name;
    }

    public int getAge() {
      return age;
    }

    @Override
    public String toString() {
      return "Person{name='" + name + "', age=" + age + '}';
    }
  }

  /**
   * 演示用的EntryListener实现
   */
  private class DemoEntryListener implements EntryListener<String, String> {
    @Override
    public void entryAdded(EntryEvent<String, String> event) {
      System.out.println("添加事件: " + event.getKey() + " -> " + event.getValue());
    }

    @Override
    public void entryEvicted(EntryEvent<String, String> event) {
      System.out.println("驱逐事件: " + event.getKey());
    }

    @Override
    public void entryRemoved(EntryEvent<String, String> event) {
      System.out.println("移除事件: " + event.getKey());
    }

    @Override
    public void entryUpdated(EntryEvent<String, String> event) {
      System.out.println("更新事件: " + event.getKey() + " -> " + event.getValue());
    }

    @Override
    public void entryExpired(EntryEvent<String, String> event) {
      System.out.println("过期事件: " + event.getKey());
    }

    @Override
    public void mapEvicted(MapEvent event) {
      System.out.println("Map驱逐事件");
    }

    @Override
    public void mapCleared(MapEvent event) {
      System.out.println("Map清除事件");
    }
  }
}