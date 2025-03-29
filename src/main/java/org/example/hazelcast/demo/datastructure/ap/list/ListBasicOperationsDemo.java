package org.example.hazelcast.demo.datastructure.ap.list;

import com.hazelcast.collection.IList;
import com.hazelcast.collection.ItemEvent;
import com.hazelcast.collection.ItemListener;
import com.hazelcast.config.CollectionConfig;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

/**
 * Hazelcast List 基本操作示例
 * 
 * 官方定义：
 * Hazelcast List (IList) 是 java.util.List 接口的分布式实现。它允许重复元素并且
 * 保持元素的插入顺序。所有操作都是线程安全的，以支持多线程应用。
 * 
 * 主要特性：
 * 1. 顺序保证：元素按插入顺序存储
 * 2. 允许重复：可以添加多个相同的元素
 * 3. 位置访问：支持基于索引的访问和操作
 * 4. 分布式存储：数据分布在集群的所有成员之间
 * 5. 线程安全：所有操作都是线程安全的
 * 6. 事件通知：支持项目添加/删除的事件监听
 * 7. 备份支持：可配置备份数量，确保高可用性
 * 8. 有界容量：可以限制列表的最大大小
 * 
 * 适用场景：
 * - 需要保持插入顺序的分布式集合
 * - 消息队列（虽然Queue更专用）
 * - 历史记录和日志存储
 * - 需要随机访问元素的序列
 * - 共享任务列表和工作项分配
 * - 有序数据处理
 * 
 * 配置选项：
 * - 备份数量：控制数据备份副本数
 * - 最大大小：限制List可以容纳的最大元素数
 * - 统计收集：启用/禁用性能统计
 * - 分裂脑保护：防止在网络分区期间数据不一致
 * 
 * List与其他集合的区别：
 * - 相比Set：List允许重复并保证顺序，Set不允许重复且不保证顺序
 * - 相比Queue：List提供随机访问，Queue专注于FIFO操作
 * - 相比Map：List是值的序列，Map是键值对的集合
 * 
 * 性能考虑：
 * - 元素查找需要线性时间(O(n))
 * - 末尾添加/删除通常是恒定时间操作
 * - 中间插入/删除需要移动元素，性能较低
 * 
 * 本示例演示了Hazelcast List的基本操作，包括添加、检索、定位操作、
 * 事件监听、配置以及子列表操作等功能。
 */
@Component
public class ListBasicOperationsDemo {

  private final HazelcastInstance hazelcastInstance;

  public ListBasicOperationsDemo(HazelcastInstance hazelcastInstance) {
    this.hazelcastInstance = hazelcastInstance;
  }

  public void runAllExamples() {
    System.out.println("\n开始List示例...");
    basicOperationsExample();
    positionOperationsExample();
    itemListenerExample();
    configurationExample();
    subListExample();
    System.out.println("\nList示例完成！");
  }

  /**
   * 基本List操作示例
   */
  public void basicOperationsExample() {
    System.out.println("\n=== List基本操作示例 ===");

    // 获取List实例
    IList<String> list = hazelcastInstance.getList("basic-list-demo");

    try {
      // 清空List，确保演示从空开始
      list.clear();

      System.out.println("添加元素到List");

      // 添加元素到List
      list.add("Tokyo");
      list.add("Paris");
      list.add("London");
      list.add("New York");

      System.out.println("List大小: " + list.size());

      // 获取元素
      String firstCity = list.get(0);
      System.out.println("\n第一个元素: " + firstCity);

      // 检查元素是否存在
      boolean contains = list.contains("Paris");
      System.out.println("是否包含 'Paris': " + contains);

      // 遍历所有元素
      System.out.println("\n所有元素(按顺序):");
      for (int i = 0; i < list.size(); i++) {
        System.out.println(i + ": " + list.get(i));
      }

      // 移除元素
      boolean removed = list.remove("London");
      System.out.println("\n移除 'London': " + removed);
      System.out.println("移除后List大小: " + list.size());

      // 批量添加
      Collection<String> cities = Arrays.asList("Berlin", "Madrid", "Rome");
      list.addAll(cities);
      System.out.println("\n批量添加后List大小: " + list.size());

      // 转换为数组
      Object[] cityArray = list.toArray();
      System.out.println("\n转换为数组后的元素数量: " + cityArray.length);

      // 检查是否为空
      boolean isEmpty = list.isEmpty();
      System.out.println("List是否为空: " + isEmpty);

    } finally {
      // 清理资源
      list.destroy();
    }
  }

  /**
   * 演示List的位置操作
   */
  public void positionOperationsExample() {
    System.out.println("\n=== List位置操作示例 ===");

    IList<String> list = hazelcastInstance.getList("position-operations-demo");

    try {
      list.clear();

      // 添加元素
      list.add("A");
      list.add("B");
      list.add("C");
      list.add("D");
      list.add("B"); // 重复元素

      System.out.println("初始List: " + list);

      // 在特定位置添加元素
      list.add(2, "X");
      System.out.println("\n在索引2处添加'X'后: " + list);

      // 获取元素索引
      int firstIndex = list.indexOf("B");
      int lastIndex = list.lastIndexOf("B");
      System.out.println("\n'B'的第一个索引: " + firstIndex);
      System.out.println("'B'的最后一个索引: " + lastIndex);

      // 设置元素
      String oldValue = list.set(3, "Y");
      System.out.println("\n将索引3处的值'" + oldValue + "'替换为'Y': " + list);

      // 按索引移除
      String removedValue = list.remove(1);
      System.out.println("\n移除索引1处的值'" + removedValue + "': " + list);

    } finally {
      list.destroy();
    }
  }

  /**
   * 演示List的项目监听器
   */
  public void itemListenerExample() {
    System.out.println("\n=== List项目监听器示例 ===");

    IList<String> list = hazelcastInstance.getList("listener-demo");

    try {
      list.clear();

      // 添加项目监听器
      System.out.println("添加项目监听器到List");
      UUID listenerId = list.addItemListener(new DemoItemListener(), true);

      System.out.println("\n执行操作触发监听器事件:");

      // 添加项目
      list.add("Item1");

      // 在指定位置添加项目
      list.add(0, "Item0");

      // 移除项目
      list.remove("Item1");

      // 移除监听器
      System.out.println("\n移除监听器");
      list.removeItemListener(listenerId);

      System.out.println("移除监听器后添加项目 (不会触发事件):");
      list.add("Item2");

      System.out.println("\nXML监听器配置示例:");
      System.out.println("<list name=\"default\">");
      System.out.println("    <item-listeners>");
      System.out.println("        <item-listener>com.example.ItemListener</item-listener>");
      System.out.println("    </item-listeners>");
      System.out.println("</list>");

    } finally {
      list.destroy();
    }
  }

  /**
   * 演示List的配置选项
   */
  public void configurationExample() {
    System.out.println("\n=== List配置示例 ===");

    System.out.println("Hazelcast List支持多种配置选项:");
    System.out.println("1. backup-count: 同步备份数量");
    System.out.println("2. async-backup-count: 异步备份数量");
    System.out.println("3. statistics-enabled: 是否启用统计");
    System.out.println("4. max-size: 最大容量限制");
    System.out.println("5. split-brain-protection-ref: 脑裂保护配置");

    System.out.println("\nXML配置示例:");
    System.out.println("<list name=\"default\">");
    System.out.println("    <statistics-enabled>false</statistics-enabled>");
    System.out.println("    <backup-count>1</backup-count>");
    System.out.println("    <async-backup-count>0</async-backup-count>");
    System.out.println("    <max-size>10</max-size>");
    System.out.println("</list>");

    System.out.println("\n注意: List配置应在Hazelcast实例启动前完成");

    // 演示一个有最大容量限制的List
    System.out.println("\n在实际使用中，max-size可以限制List的大小");
    System.out.println("当超过最大大小时，会拒绝添加新元素");
  }

  /**
   * 演示List的子列表操作
   */
  public void subListExample() {
    System.out.println("\n=== List子列表操作示例 ===");

    IList<String> list = hazelcastInstance.getList("sublist-demo");

    try {
      list.clear();

      // 添加多个元素
      for (int i = 0; i < 10; i++) {
        list.add("Item" + i);
      }

      System.out.println("原始List: " + list);

      // 获取子列表
      List<String> subList = list.subList(2, 7);
      System.out.println("\n子列表(索引2到6): " + subList);

      // 注意: subList返回的是原始列表的视图，对其修改会反映到原始列表
      System.out.println("\n子列表是原始列表的视图，对子列表的修改会影响原始列表");
      System.out.println("例如：移除子列表中的元素也会从原始列表中移除该元素");

      // 检查多个元素是否包含在列表中
      boolean containsAll = list.containsAll(Arrays.asList("Item1", "Item2"));
      System.out.println("\nList是否包含所有元素['Item1', 'Item2']: " + containsAll);

    } finally {
      list.destroy();
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