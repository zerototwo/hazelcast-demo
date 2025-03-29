package org.example.hazelcast.demo.datastructure.ap.set;

import com.hazelcast.collection.ISet;
import com.hazelcast.collection.ItemEvent;
import com.hazelcast.collection.ItemListener;
import com.hazelcast.config.Config;
import com.hazelcast.config.SetConfig;
import com.hazelcast.core.HazelcastInstance;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Hazelcast Set 基本操作示例
 * 
 * 官方定义：
 * Hazelcast Set (ISet) 是标准 java.util.Set 的分布式实现。不允许重复元素，
 * 并且不保证存储的顺序。它是线程安全的并且提供集合相关的所有操作。
 * 
 * 主要特性：
 * 1. 唯一元素：不允许重复元素
 * 2. 分布式存储：数据分布在集群的所有成员之间
 * 3. 数据分区：数据根据哈希值自动分布到不同节点
 * 4. 无序性：不保证元素的存储顺序
 * 5. 事件通知：支持在添加/删除元素时触发事件
 * 6. 线程安全：所有操作都是线程安全的
 * 7. 备份支持：数据可以配置备份数，提高可用性
 * 8. 集合操作：支持标准的集合操作（并集、交集、差集）
 * 
 * 适用场景：
 * - 需要唯一元素集合的分布式应用
 * - 用户权限管理
 * - 唯一标识符存储
 * - 对集合需要执行交集、并集运算的场景
 * - 分布式白名单/黑名单
 * - 避免重复处理的应用
 * 
 * 配置选项：
 * - 备份数量：控制数据备份副本数
 * - 最大容量：设置集合的最大元素数
 * - 统计收集：启用/禁用性能统计
 * - 分裂保护：防止在网络分区下数据不一致
 * 
 * Set 与 List 的区别：
 * - Set 不允许重复元素，List 允许
 * - Set 不保证顺序，List 保证插入顺序
 * - Set 查找元素性能通常更好
 * 
 * 本示例演示了 Hazelcast Set 的基本操作，包括添加、删除、
 * 集合运算以及事件监听等功能。
 */
@Component
public class SetBasicOperationsDemo {

  private final HazelcastInstance hazelcastInstance;

  public SetBasicOperationsDemo(HazelcastInstance hazelcastInstance) {
    this.hazelcastInstance = hazelcastInstance;
  }

  public void runAllExamples() {
    System.out.println("\n开始Set示例...");
    basicOperationsExample();
    itemListenerExample();
    configurationExample();
    setOperationsExample();
    System.out.println("\nSet示例完成！");
  }

  /**
   * 基本Set操作示例
   */
  public void basicOperationsExample() {
    System.out.println("\n=== Set基本操作示例 ===");

    // 获取Set实例
    ISet<String> set = hazelcastInstance.getSet("basic-set-demo");

    try {
      // 清空Set，确保演示从空开始
      set.clear();

      System.out.println("添加元素到Set");

      // 添加元素到Set
      set.add("Tokyo");
      set.add("Paris");
      set.add("London");
      set.add("New York");

      System.out.println("Set大小: " + set.size());

      // 尝试添加重复元素 - 不会添加成功
      boolean added = set.add("Tokyo");
      System.out.println("尝试添加重复元素 'Tokyo': " + added);
      System.out.println("Set大小(添加重复后): " + set.size());

      // 检查元素是否存在
      boolean contains = set.contains("Paris");
      System.out.println("\n是否包含 'Paris': " + contains);

      // 获取所有元素
      System.out.println("\n所有元素:");
      for (String city : set) {
        System.out.println("- " + city);
      }

      // 移除元素
      boolean removed = set.remove("London");
      System.out.println("\n移除 'London': " + removed);
      System.out.println("移除后Set大小: " + set.size());

      // 批量添加
      Collection<String> cities = Arrays.asList("Berlin", "Madrid", "Rome");
      set.addAll(cities);
      System.out.println("\n批量添加后Set大小: " + set.size());

      // 转换为数组
      Object[] cityArray = set.toArray();
      System.out.println("\n转换为数组后的元素数量: " + cityArray.length);

      // 检查是否为空
      boolean isEmpty = set.isEmpty();
      System.out.println("Set是否为空: " + isEmpty);

    } finally {
      // 清理资源
      set.destroy();
    }
  }

  /**
   * 演示Set的项目监听器
   */
  public void itemListenerExample() {
    System.out.println("\n=== Set项目监听器示例 ===");

    ISet<String> set = hazelcastInstance.getSet("listener-demo");

    try {
      set.clear();

      // 添加项目监听器
      System.out.println("添加项目监听器到Set");
      UUID listenerId = set.addItemListener(new DemoItemListener(), true);

      System.out.println("\n执行操作触发监听器事件:");

      // 添加项目
      set.add("Item1");

      // 添加另一个项目
      set.add("Item2");

      // 移除项目
      set.remove("Item1");

      // 移除监听器
      System.out.println("\n移除监听器");
      set.removeItemListener(listenerId);

      System.out.println("移除监听器后添加项目 (不会触发事件):");
      set.add("Item3");

      System.out.println("\nXML监听器配置示例:");
      System.out.println("<set name=\"default\">");
      System.out.println("    <item-listeners>");
      System.out.println("        <item-listener>com.example.ItemListener</item-listener>");
      System.out.println("    </item-listeners>");
      System.out.println("</set>");

    } finally {
      set.destroy();
    }
  }

  /**
   * 演示Set的配置选项
   */
  public void configurationExample() {
    System.out.println("\n=== Set配置示例 ===");

    System.out.println("Hazelcast Set支持多种配置选项:");
    System.out.println("1. backup-count: 同步备份数量");
    System.out.println("2. async-backup-count: 异步备份数量");
    System.out.println("3. statistics-enabled: 是否启用统计");
    System.out.println("4. max-size: 最大容量限制 (0表示无限制)");
    System.out.println("5. split-brain-protection-ref: 脑裂保护配置");

    System.out.println("\nXML配置示例:");
    System.out.println("<set name=\"default\">");
    System.out.println("    <statistics-enabled>false</statistics-enabled>");
    System.out.println("    <backup-count>1</backup-count>");
    System.out.println("    <async-backup-count>0</async-backup-count>");
    System.out.println("    <max-size>10</max-size>");
    System.out.println("</set>");

    System.out.println("\n注意: 集配置应在Hazelcast实例启动前完成");

    // 演示一个有最大容量限制的Set
    System.out.println("\n在实际使用中，max-size可以限制Set的大小");
    System.out.println("当超过最大大小时，会拒绝添加新元素");
  }

  /**
   * 演示Set特有的集合操作
   */
  public void setOperationsExample() {
    System.out.println("\n=== Set集合操作示例 ===");

    ISet<String> set1 = hazelcastInstance.getSet("set-operations-1");
    ISet<String> set2 = hazelcastInstance.getSet("set-operations-2");

    try {
      set1.clear();
      set2.clear();

      // 添加元素到第一个Set
      set1.add("A");
      set1.add("B");
      set1.add("C");

      // 添加元素到第二个Set
      set2.add("B");
      set2.add("C");
      set2.add("D");

      System.out.println("Set1: " + set1);
      System.out.println("Set2: " + set2);

      // 检查包含关系
      boolean containsAll = set1.containsAll(Arrays.asList("A", "B"));
      System.out.println("\nSet1是否包含所有元素['A', 'B']: " + containsAll);

      // 演示保留操作
      Collection<String> retainItems = Arrays.asList("B", "C");
      System.out.println("\n在Set1中只保留元素: " + retainItems);
      set1.retainAll(retainItems);
      System.out.println("保留操作后Set1: " + set1);

      // 演示移除操作
      Collection<String> removeItems = Arrays.asList("C");
      System.out.println("\n从Set1中移除元素: " + removeItems);
      set1.removeAll(removeItems);
      System.out.println("移除操作后Set1: " + set1);

    } finally {
      set1.destroy();
      set2.destroy();
    }
  }

  /**
   * 演示用的ItemListener实现
   */
  private static class DemoItemListener implements ItemListener<String> {
    @Override
    public void itemAdded(ItemEvent<String> item) {
      System.out.println("添加项目: " + item.getItem());
    }

    @Override
    public void itemRemoved(ItemEvent<String> item) {
      System.out.println("移除项目: " + item.getItem());
    }
  }
}