package org.example.hazelcast.demo.ap.multimap;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.multimap.MultiMap;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Hazelcast MultiMap 基本操作示例
 * 
 * 官方定义：
 * Hazelcast MultiMap是专门用于存储一个键对应多个值的特殊分布式Map实现。
 * 普通Map每个键只能存储一个值，而MultiMap允许将多个值与单个键关联。
 * 
 * 主要特性：
 * 1. 一对多映射：单个键可以关联多个值
 * 2. 集合类型：支持将值作为集合(Set)或列表(List)存储
 * 3. 分布式存储：数据分布在集群的多个节点上
 * 4. 备份支持：提供数据备份以保护数据不丢失
 * 5. 事件监听：支持添加和删除事件的监听
 * 6. 锁支持：提供基于条目的锁定功能
 * 7. 原子操作：提供原子操作来保证一致性
 * 
 * 常见用例：
 * - 用户与角色的多对多关系
 * - 索引结构（如倒排索引）
 * - 标签系统（一个项目多个标签）
 * - 目录结构（一个父目录多个子项）
 * - 网络图结构（如社交网络中的关系）
 * 
 * 使用场景区别：
 * - 当需要一个键关联多个值时选择MultiMap
 * - 当只需一个键对应一个（可能是复杂的）值时选择Map
 * 
 * 本示例演示了Hazelcast MultiMap的基本操作，包括创建、添加多个值、
 * 检索、删除等操作，以及值集合类型和锁定功能的使用。
 */
@Component
public class MultiMapBasicOperationsDemo {

  private final HazelcastInstance hazelcastInstance;
  private final String MULTI_MAP_NAME = "demo-multi-map";

  public MultiMapBasicOperationsDemo(HazelcastInstance hazelcastInstance) {
    this.hazelcastInstance = hazelcastInstance;
  }

  public void runAllExamples() {
    System.out.println("\n开始MultiMap示例...");
    basicOperationsExample();
    collectionTypeExample();
    lockingExample();
    listenersExample();
    System.out.println("\nMultiMap示例完成！");
  }

  /**
   * MultiMap基本操作示例
   */
  public void basicOperationsExample() {
    System.out.println("\n=== MultiMap基本操作示例 ===");

    // 获取MultiMap实例
    MultiMap<String, String> multiMap = hazelcastInstance.getMultiMap("basic-multimap-demo");

    try {
      // 清空MultiMap，确保演示从空开始
      multiMap.clear();

      System.out.println("添加键值对到MultiMap");

      // 向同一个键添加多个值
      multiMap.put("fruits", "apple");
      multiMap.put("fruits", "banana");
      multiMap.put("fruits", "orange");

      multiMap.put("vegetables", "carrot");
      multiMap.put("vegetables", "potato");

      System.out.println("MultiMap大小: " + multiMap.size());
      System.out.println("键的数量: " + multiMap.keySet().size());

      // 获取某个键的所有值
      Collection<String> fruits = multiMap.get("fruits");
      System.out.println("\n'fruits'键对应的值: " + fruits);
      System.out.println("'fruits'键对应的值数量: " + multiMap.valueCount("fruits"));

      // 检查键值对是否存在
      boolean containsApple = multiMap.containsEntry("fruits", "apple");
      System.out.println("\n是否包含键值对 'fruits'-'apple': " + containsApple);

      // 移除特定的键值对
      boolean removed = multiMap.remove("fruits", "banana");
      System.out.println("移除键值对 'fruits'-'banana': " + removed);
      System.out.println("移除后 'fruits'键对应的值: " + multiMap.get("fruits"));

      // 获取所有值
      Collection<String> allValues = multiMap.values();
      System.out.println("\n所有值: " + allValues);

      // 获取所有键值对
      Set<Map.Entry<String, String>> entries = multiMap.entrySet();
      System.out.println("\n所有键值对:");
      for (Map.Entry<String, String> entry : entries) {
        System.out.println(entry.getKey() + " -> " + entry.getValue());
      }

      // 删除某个键的所有值
      Collection<String> removedValues = multiMap.remove("vegetables");
      System.out.println("\n移除'vegetables'键及其所有值: " + removedValues);
      System.out.println("移除后MultiMap大小: " + multiMap.size());

    } finally {
      // 清理资源
      multiMap.destroy();
    }
  }

  /**
   * 演示不同的值集合类型
   */
  public void collectionTypeExample() {
    System.out.println("\n=== MultiMap值集合类型示例 ===");
    System.out.println("注意: 在实际应用中，集合类型需要在配置文件中设置为SET或LIST");
    System.out.println("此示例展示了集合类型的概念，但实际类型由配置决定");

    // 在Hazelcast中，MultiMap的集合类型在配置中设置，默认为SET
    MultiMap<String, String> multiMap = hazelcastInstance.getMultiMap("collection-type-demo");

    try {
      multiMap.clear();

      // 添加重复值 - 如果配置为SET类型则会被去重
      multiMap.put("colors", "red");
      multiMap.put("colors", "green");
      multiMap.put("colors", "blue");
      multiMap.put("colors", "red"); // 重复值

      Collection<String> colors = multiMap.get("colors");
      System.out.println("\n'colors'键对应的值: " + colors);
      System.out.println("值的数量: " + colors.size());

      if (colors.size() < 4) {
        System.out.println("注意: 重复值('red')被去除 - 这表明当前配置的集合类型是SET");
      } else {
        System.out.println("注意: 重复值('red')被保留 - 这表明当前配置的集合类型是LIST");
      }

      System.out.println("\n如何在配置中设置集合类型:");
      System.out.println("XML示例:");
      System.out.println("<multimap name=\"default\">");
      System.out.println("    <value-collection-type>SET</value-collection-type>");
      // 或者
      System.out.println("    <!-- <value-collection-type>LIST</value-collection-type> -->");
      System.out.println("</multimap>");

    } finally {
      multiMap.destroy();
    }
  }

  /**
   * 演示MultiMap的锁定功能
   */
  public void lockingExample() {
    System.out.println("\n=== MultiMap锁定示例 ===");

    MultiMap<String, Integer> multiMap = hazelcastInstance.getMultiMap("locking-demo");

    try {
      multiMap.clear();

      // 添加一些初始数据
      multiMap.put("scores", 100);
      multiMap.put("scores", 85);
      multiMap.put("scores", 90);

      System.out.println("原始'scores'值: " + multiMap.get("scores"));

      // 锁定键
      System.out.println("\n锁定'scores'键...");
      multiMap.lock("scores");

      try {
        System.out.println("'scores'键是否被锁定: " + multiMap.isLocked("scores"));

        // 在锁定状态下修改值
        multiMap.remove("scores", 85);
        multiMap.put("scores", 95);

        System.out.println("锁定后修改的'scores'值: " + multiMap.get("scores"));

      } finally {
        // 解锁
        System.out.println("\n解锁'scores'键");
        multiMap.unlock("scores");
        System.out.println("'scores'键是否被锁定: " + multiMap.isLocked("scores"));
      }

      // 尝试使用超时锁定
      try {
        boolean locked = multiMap.tryLock("scores", 1, TimeUnit.SECONDS);
        if (locked) {
          try {
            System.out.println("\n成功使用tryLock获取锁");
            multiMap.put("scores", 105);
          } finally {
            multiMap.unlock("scores");
          }
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        System.err.println("获取锁时被中断: " + e.getMessage());
      }

      System.out.println("最终'scores'值: " + multiMap.get("scores"));

    } finally {
      multiMap.destroy();
    }
  }

  /**
   * 演示如何使用MultiMap的监听器
   * 注意：真实情况下，监听器通常在应用启动时注册，而不是在示例方法中
   */
  public void listenersExample() {
    System.out.println("\n=== MultiMap监听器示例 ===");
    System.out.println("注意: 此示例展示了监听器的概念，但实际监听效果取决于配置和集群环境");

    MultiMap<String, String> multiMap = hazelcastInstance.getMultiMap("listeners-demo");

    try {
      multiMap.clear();

      System.out.println("在实际应用中，您可以为MultiMap添加条目监听器");
      System.out.println("监听器可以响应添加、移除等事件");

      System.out.println("\n配置监听器的XML示例:");
      System.out.println("<multimap name=\"default\">");
      System.out.println("    <entry-listeners>");
      System.out.println(
          "        <entry-listener include-value=\"true\" local=\"false\">com.example.MyEntryListener</entry-listener>");
      System.out.println("    </entry-listeners>");
      System.out.println("</multimap>");

      System.out.println("\n监听器实现示例:");
      System.out.println("public class MyEntryListener implements EntryListener<String, String> {");
      System.out.println("    @Override");
      System.out.println("    public void entryAdded(EntryEvent<String, String> event) {");
      System.out.println("        System.out.println(\"添加: \" + event.getKey() + \" -> \" + event.getValue());");
      System.out.println("    }");
      System.out.println("    // 其他方法实现...");
      System.out.println("}");

      // 模拟一些操作以展示可能触发的事件
      System.out.println("\n执行一些可能触发监听器事件的操作:");

      multiMap.put("events", "created");
      System.out.println("添加: 'events' -> 'created'");

      multiMap.put("events", "updated");
      System.out.println("添加: 'events' -> 'updated'");

      multiMap.remove("events", "created");
      System.out.println("移除: 'events' -> 'created'");

    } finally {
      multiMap.destroy();
    }
  }
}