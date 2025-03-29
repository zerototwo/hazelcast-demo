package org.example.hazelcast.demo.compute.entryprocessor;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.map.EntryProcessor;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * 演示Hazelcast EntryProcessor的使用方法。
 * 
 * EntryProcessor是Hazelcast提供的一个强大特性，允许直接在存储数据的集群成员上执行计算，
 * 而不是先将数据取回客户端，修改后再发送回去。这种方式可以显著减少网络传输并提高性能。
 * 
 * 主要特点：
 * - 原子操作：EntryProcessor的执行是原子的，无需显式加锁
 * - 减少网络传输：计算在数据所在的节点上进行
 * - 支持批量操作：可以在多个键上同时执行相同的处理
 */
@Component
public class EntryProcessorDemo {

  private final HazelcastInstance hazelcastInstance;

  public EntryProcessorDemo(HazelcastInstance hazelcastInstance) {
    this.hazelcastInstance = hazelcastInstance;
  }

  /**
   * 运行所有EntryProcessor示例
   */
  public void runAllExamples() {
    System.out.println("===== Running All EntryProcessor Examples =====");

    basicEntryProcessorExample();
    System.out.println();

    batchEntryProcessorExample();
    System.out.println();

    conditionalUpdateExample();
    System.out.println();

    returningResultsExample();
    System.out.println("===============================================");
  }

  /**
   * 基本EntryProcessor示例
   * 展示如何在单个Map条目上执行处理器
   */
  public void basicEntryProcessorExample() {
    System.out.println("--- Basic EntryProcessor Example ---");

    // 获取或创建一个Map
    IMap<String, Integer> map = hazelcastInstance.getMap("demo-entry-processor-map");

    // 初始化数据
    map.put("counter", 0);
    System.out.println("Initial value: " + map.get("counter"));

    // 创建并执行EntryProcessor
    // 此处使用lambda表达式，也可以使用实现了EntryProcessor接口的类
    map.executeOnKey("counter", (EntryProcessor<String, Integer, Void>) entry -> {
      // 获取当前值
      Integer value = entry.getValue();
      // 更新值
      entry.setValue(value + 1);
      return null; // 本例不需要返回值
    });

    System.out.println("After increment: " + map.get("counter"));

    // 清理
    map.delete("counter");
  }

  /**
   * 批量EntryProcessor示例
   * 展示如何在多个Map条目上同时执行相同的处理
   */
  public void batchEntryProcessorExample() {
    System.out.println("--- Batch EntryProcessor Example ---");

    // 获取或创建一个Map
    IMap<String, Integer> map = hazelcastInstance.getMap("demo-entry-processor-map");

    // 初始化数据
    map.put("item1", 10);
    map.put("item2", 20);
    map.put("item3", 30);

    System.out.println("Initial values:");
    map.forEach((k, v) -> System.out.println(k + " = " + v));

    // 创建增加值的EntryProcessor
    EntryProcessor<String, Integer, Void> incrementor = entry -> {
      entry.setValue(entry.getValue() + 5);
      return null;
    };

    // 在多个键上执行相同的EntryProcessor
    Set<String> keys = Set.of("item1", "item2", "item3");
    map.executeOnKeys(keys, incrementor);

    System.out.println("After increment by 5:");
    map.forEach((k, v) -> System.out.println(k + " = " + v));

    // 清理
    keys.forEach(map::delete);
  }

  /**
   * 条件更新示例
   * 展示如何在EntryProcessor中根据条件决定是否更新条目
   */
  public void conditionalUpdateExample() {
    System.out.println("--- Conditional Update Example ---");

    // 获取或创建一个Map
    IMap<String, Integer> map = hazelcastInstance.getMap("demo-entry-processor-map");

    // 初始化数据
    map.put("score1", 75);
    map.put("score2", 45);
    map.put("score3", 90);

    System.out.println("Initial scores:");
    map.forEach((k, v) -> System.out.println(k + " = " + v));

    // 创建条件更新处理器：只有及格分数才加奖励分
    EntryProcessor<String, Integer, Boolean> bonusProcessor = entry -> {
      Integer score = entry.getValue();
      // 只有及格 (>= 60) 才加奖励分
      if (score >= 60) {
        entry.setValue(score + 10);
        return true; // 返回true表示此条目已更新
      }
      return false; // 返回false表示此条目未更新
    };

    // 在所有条目上执行条件更新
    Map<String, Boolean> results = map.executeOnEntries(bonusProcessor);

    System.out.println("Update results (which entries were updated):");
    results.forEach((k, v) -> System.out.println(k + " updated: " + v));

    System.out.println("Scores after bonus:");
    map.forEach((k, v) -> System.out.println(k + " = " + v));

    // 清理
    map.clear();
  }

  /**
   * 返回结果示例
   * 展示如何从EntryProcessor获取返回值
   */
  public void returningResultsExample() {
    System.out.println("--- Returning Results Example ---");

    // 获取或创建一个Map
    IMap<String, Integer> map = hazelcastInstance.getMap("demo-entry-processor-map");

    // 初始化产品数据
    map.put("product1", 100); // 价格
    map.put("product2", 200);
    map.put("product3", 300);

    System.out.println("Initial prices:");
    map.forEach((k, v) -> System.out.println(k + " = $" + v));

    // 创建折扣处理器：应用20%折扣并返回原价
    EntryProcessor<String, Integer, Integer> discountProcessor = entry -> {
      Integer originalPrice = entry.getValue();
      // 应用20%折扣
      int discountedPrice = (int) (originalPrice * 0.8);
      entry.setValue(discountedPrice);
      return originalPrice; // 返回原价用于比较
    };

    // 在多个键上执行并获取结果
    Map<String, Integer> originalPrices = new HashMap<>();
    Set<String> keys = Set.of("product1", "product2", "product3");

    for (String key : keys) {
      Integer originalPrice = map.executeOnKey(key, discountProcessor);
      originalPrices.put(key, originalPrice);
    }

    System.out.println("Price comparison (after 20% discount):");
    map.forEach((k, v) -> {
      Integer original = originalPrices.get(k);
      System.out.printf("%s: Original $%d, Discounted $%d, Saved $%d%n",
          k, original, v, original - v);
    });

    // 清理
    map.clear();
  }
}