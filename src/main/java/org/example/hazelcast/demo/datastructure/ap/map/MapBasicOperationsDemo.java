package org.example.hazelcast.demo.datastructure.ap.map;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import org.example.hazelcast.demo.model.Product;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Hazelcast Map 基本操作示例
 *
 * Hazelcast Map(IMap)是分布式键值数据结构，等同于Java Map，但提供了更多功能：
 * 
 * 官方定义：
 * Hazelcast Map是分布式实现，在集群的所有成员间进行分区。数据在集群中均匀分布，
 * 使得Map可以水平扩展以提供近乎线性的性能和内存使用增长。
 * 
 * 主要特性：
 * 1. 分布式存储：数据分布在集群的多个节点上，提供自动扩展能力
 * 2. 内存读取/持久化：可配置为纯内存模式或持久化到外部存储
 * 3. 数据分区：使用一致性哈希算法将数据分区到不同节点
 * 4. 备份：支持同步/异步备份，保证数据安全性
 * 5. 事件监听：支持添加、更新、删除等事件监听
 * 6. 查询能力：支持Predicate查询、SQL查询、全文搜索
 * 7. 近数据处理：通过EntryProcessor在数据所在节点执行计算
 * 8. 过期和淘汰：支持基于时间或容量的自动过期和淘汰策略
 * 
 * 常见用例：
 * - 分布式缓存
 * - 分布式数据存储
 * - 集群内数据共享
 * - 大规模数据处理系统
 * - 用户会话存储
 * 
 * 本示例演示了Hazelcast Map的基本操作，包括创建、添加、获取、更新和删除等功能。
 */
@Component
public class MapBasicOperationsDemo {

  private final HazelcastInstance hazelcastInstance;
  private final String MAP_NAME = "products";

  @Autowired
  public MapBasicOperationsDemo(HazelcastInstance hazelcastInstance) {
    this.hazelcastInstance = hazelcastInstance;
  }

  /**
   * 运行所有基本操作示例
   */
  public void runAllExamples() {
    System.out.println("=================== Hazelcast Map 基本操作示例 ===================");
//    putExample();
//    getExample();
//    containsKeyExample();
//    updateExample();
//    removeExample();
//    clearExample();
  }

  /**
   * 添加条目示例
   */
  public void putExample() {
    System.out.println("\n--- Map.put() 方法示例 ---");

    // 获取Map
    IMap<Long, Product> productMap = hazelcastInstance.getMap(MAP_NAME);

    // 添加条目
    Product product1 = new Product(1L, "笔记本电脑", "电子产品", new BigDecimal("5999.00"), 100);
    productMap.put(product1.getId(), product1);
    System.out.println("添加了产品: " + product1);

    // 添加条目并获取旧值(如果有)
    Product product2 = new Product(2L, "智能手机", "电子产品", new BigDecimal("3999.00"), 200);
    Product oldValue = productMap.put(product2.getId(), product2);
    System.out.println("添加了产品: " + product2 + ", 旧值: " + oldValue);

    // 添加条目，设置生存时间
    Product product3 = new Product(3L, "耳机", "配件", new BigDecimal("299.00"), 500);
    productMap.put(product3.getId(), product3, 30, TimeUnit.SECONDS);
    System.out.println("添加了具有30秒TTL的产品: " + product3);

    // 仅在key不存在时添加(putIfAbsent)
    Product product4 = new Product(4L, "平板电脑", "电子产品", new BigDecimal("2999.00"), 150);
    Product existing = productMap.putIfAbsent(product4.getId(), product4);
    System.out.println("putIfAbsent: " + (existing == null ? "添加成功" : "已存在，添加失败"));

    // 再次调用putIfAbsent，此时key已存在
    Product product4Duplicate = new Product(4L, "平板电脑Pro", "电子产品", new BigDecimal("3999.00"), 150);
    existing = productMap.putIfAbsent(product4.getId(), product4Duplicate);
    System.out.println("再次putIfAbsent: " + (existing == null ? "添加成功" : "已存在，添加失败，现有值: " + existing.getName()));
  }

  /**
   * 获取条目示例
   */
  public void getExample() {
    System.out.println("\n--- Map.get() 方法示例 ---");

    IMap<Long, Product> productMap = hazelcastInstance.getMap(MAP_NAME);

    // 获取条目
    Product product = productMap.get(1L);
    System.out.println("获取ID为1的产品: " + product);

    // 获取不存在的条目
    Product nonExistent = productMap.get(999L);
    System.out.println("获取不存在的产品: " + nonExistent);

    // 获取条目集合
    Set<Long> keys = productMap.keySet();
    System.out.println("所有产品ID: " + keys);

    // 获取所有值
    System.out.println("所有产品:");
    for (Product p : productMap.values()) {
      System.out.println(" - " + p);
    }

    // 获取条目并加载(如果配置了MapLoader)
    product = productMap.getOrDefault(5L, new Product(5L, "默认产品", "默认类别", BigDecimal.ZERO, 0));
    System.out.println("getOrDefault 结果: " + product);
  }

  /**
   * 包含键检查示例
   */
  public void containsKeyExample() {
    System.out.println("\n--- Map.containsKey() 和 containsValue() 方法示例 ---");

    IMap<Long, Product> productMap = hazelcastInstance.getMap(MAP_NAME);

    // 检查键是否存在
    boolean hasKey1 = productMap.containsKey(1L);
    System.out.println("Map包含键1: " + hasKey1);

    boolean hasKey999 = productMap.containsKey(999L);
    System.out.println("Map包含键999: " + hasKey999);

    // 检查值是否存在 (注意: 这个操作代价很高)
    Product testProduct = new Product(1L, "笔记本电脑", "电子产品", new BigDecimal("5999.00"), 100);
    boolean containsProduct = productMap.containsValue(testProduct);
    System.out.println("Map包含值testProduct: " + containsProduct);
  }

  /**
   * 更新条目示例
   */
  public void updateExample() {
    System.out.println("\n--- Map更新操作示例 ---");

    IMap<Long, Product> productMap = hazelcastInstance.getMap(MAP_NAME);

    // 直接更新
    Product updatedProduct = new Product(1L, "高性能笔记本电脑", "电子产品", new BigDecimal("7999.00"), 50);
    productMap.put(updatedProduct.getId(), updatedProduct);
    System.out.println("更新后的产品: " + productMap.get(1L));

    // 使用replace操作
    Product oldProduct = productMap.get(2L);
    Product newProduct = new Product(2L, "旗舰智能手机", "电子产品", new BigDecimal("4999.00"), 100);
    boolean replaced = productMap.replace(newProduct.getId(), oldProduct, newProduct);
    System.out.println("使用replace(key, oldValue, newValue)更新结果: " + replaced);
    System.out.println("更新后的产品: " + productMap.get(2L));

    // 通过异步更新
    productMap.putAsync(3L, new Product(3L, "无线耳机", "配件", new BigDecimal("399.00"), 300))
        .thenAccept(oldValue -> System.out.println("异步更新成功，旧值: " + oldValue));
  }

  /**
   * 删除条目示例
   */
  public void removeExample() {
    System.out.println("\n--- Map删除操作示例 ---");

    IMap<Long, Product> productMap = hazelcastInstance.getMap(MAP_NAME);

    // 输出当前大小
    System.out.println("删除前Map大小: " + productMap.size());

    // 简单删除
    Product removed = productMap.remove(3L);
    System.out.println("删除ID为3的产品: " + removed);

    // 条件删除
    Product product4 = productMap.get(4L);
    boolean removed2 = productMap.remove(4L, product4);
    System.out.println("条件删除结果: " + removed2);

    // 异步删除
    productMap.removeAsync(1L)
        .thenAccept(oldValue -> System.out.println("异步删除成功，被删除的产品: " + oldValue));

    // 输出当前大小
    // 注意：由于异步操作，这里可能不会立即反映所有删除
    System.out.println("删除后Map大小: " + productMap.size());
  }

  /**
   * 批量操作示例
   */
  public void batchExample() {
    System.out.println("\n--- Map批量操作示例 ---");

    IMap<Long, Product> productMap = hazelcastInstance.getMap(MAP_NAME);

    // 批量添加
    Map<Long, Product> batchProducts = new HashMap<>();
    batchProducts.put(5L, new Product(5L, "键盘", "配件", new BigDecimal("199.00"), 300));
    batchProducts.put(6L, new Product(6L, "鼠标", "配件", new BigDecimal("99.00"), 400));
    batchProducts.put(7L, new Product(7L, "显示器", "电子产品", new BigDecimal("1499.00"), 100));

    productMap.putAll(batchProducts);
    System.out.println("批量添加后Map大小: " + productMap.size());

    // 批量获取
    Map<Long, Product> retrieved = productMap.getAll(Set.of(5L, 6L, 7L));
    System.out.println("批量获取结果大小: " + retrieved.size());
    retrieved.forEach((k, v) -> System.out.println("键: " + k + ", 值: " + v));
  }

  /**
   * 清空Map示例
   */
  public void clearExample() {
    System.out.println("\n--- Map.clear()示例 ---");

    IMap<Long, Product> productMap = hazelcastInstance.getMap(MAP_NAME);

    // 输出当前大小
    System.out.println("清空前Map大小: " + productMap.size());

    // 清空Map
    productMap.clear();

    // 输出清空后大小
    System.out.println("清空后Map大小: " + productMap.size());
  }
}