package org.example.hazelcast.demo.datastructure.ap.map;

import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.map.MapEvent;
import com.hazelcast.map.listener.EntryAddedListener;
import com.hazelcast.map.listener.EntryRemovedListener;
import com.hazelcast.map.listener.EntryUpdatedListener;
import com.hazelcast.map.listener.MapClearedListener;
import com.hazelcast.map.listener.MapEvictedListener;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.Predicates;
import org.example.hazelcast.demo.model.Product;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Hazelcast Map 监听器和事件处理示例
 */
@Component
public class MapListenersDemo {

  private final HazelcastInstance hazelcastInstance;
  private final String MAP_NAME = "products";

  @Autowired
  public MapListenersDemo(HazelcastInstance hazelcastInstance) {
    this.hazelcastInstance = hazelcastInstance;
  }

  /**
   * 运行所有监听器示例
   */
  public void runAllExamples() {
    System.out.println("=================== Hazelcast Map 监听器和事件处理示例 ===================");

    // 准备测试数据
    prepareData();

    // 运行各种监听器示例
    basicListenerDemo();
    predicateListenerDemo();
    individualListenersDemo();
    keyListenerDemo();
    structuralChangesListenerDemo();
    temporaryListenerDemo();
  }

  /**
   * 准备示例数据
   */
  private void prepareData() {
    IMap<Long, Product> productMap = hazelcastInstance.getMap(MAP_NAME);
    productMap.clear();
    productMap.put(1L, new Product(1L, "笔记本电脑", "电子产品", new BigDecimal("5999.00"), 100));
  }

  /**
   * 基本监听器示例
   */
  public void basicListenerDemo() {
    System.out.println("\n--- 基本监听器示例 ---");

    IMap<Long, Product> productMap = hazelcastInstance.getMap(MAP_NAME);

    // 注册监听所有事件的监听器
    UUID listenerId = productMap.addEntryListener(new EntryListenerAdapter(), true);

    System.out.println("已注册基本监听器，监听器ID: " + listenerId);

    // 执行各种操作以触发事件
    System.out.println("\n执行操作触发事件...");

    // 添加条目
    Product newProduct = new Product(2L, "智能手机", "电子产品", new BigDecimal("3999.00"), 200);
    productMap.put(newProduct.getId(), newProduct);

    // 更新条目
    newProduct.setPrice(new BigDecimal("3899.00"));
    productMap.put(newProduct.getId(), newProduct);

    // 删除条目
    productMap.remove(newProduct.getId());

    // 等待事件处理完成
    try {
      Thread.sleep(1000);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }

    // 移除监听器
    boolean removed = productMap.removeEntryListener(listenerId);
    System.out.println("监听器移除结果: " + removed);
  }

  /**
   * 带谓词的监听器示例
   */
  public void predicateListenerDemo() {
    System.out.println("\n--- 带谓词的监听器示例 ---");

    IMap<Long, Product> productMap = hazelcastInstance.getMap(MAP_NAME);

    // 创建谓词
    Predicate<Long, Product> electronicsPredicate = Predicates.equal("category", "电子产品");

    // 注册带谓词的监听器
    UUID predicateListenerId = productMap.addEntryListener(new EntryListenerAdapter("带谓词的监听器"), electronicsPredicate,
        true);

    System.out.println("已注册带谓词的监听器，只监听电子产品类别的事件");

    // 添加匹配谓词的条目
    Product electronicProduct = new Product(3L, "平板电脑", "电子产品", new BigDecimal("2999.00"), 150);
    productMap.put(electronicProduct.getId(), electronicProduct);

    // 添加不匹配谓词的条目
    Product nonElectronicProduct = new Product(4L, "书籍", "文具", new BigDecimal("99.00"), 500);
    productMap.put(nonElectronicProduct.getId(), nonElectronicProduct);

    try {
      Thread.sleep(1000);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }

    // 移除监听器
    boolean removed = productMap.removeEntryListener(predicateListenerId);
    System.out.println("带谓词的监听器移除结果: " + removed);
  }

  /**
   * 单独事件监听器示例
   */
  public void individualListenersDemo() {
    System.out.println("\n--- 单独事件监听器示例 ---");

    IMap<Long, Product> productMap = hazelcastInstance.getMap(MAP_NAME);

    // 创建自定义监听器
    EntryAddedListener<Long, Product> addedListener = event -> System.out.println("[添加监听器] 添加事件: " + event);

    EntryUpdatedListener<Long, Product> updatedListener = event -> System.out.println("[更新监听器] 更新事件: " + event);

    EntryRemovedListener<Long, Product> removedListener = event -> System.out.println("[删除监听器] 删除事件: " + event);

    MapClearedListener clearedListener = event -> System.out.println("[清空监听器] 清空事件: " + event);

    MapEvictedListener evictedListener = event -> System.out.println("[淘汰监听器] 淘汰事件: " + event);

    // 注册单独事件监听器
    UUID addListenerId = productMap.addEntryListener(addedListener, true);
    UUID updateListenerId = productMap.addEntryListener(updatedListener, true);
    UUID removeListenerId = productMap.addEntryListener(removedListener, true);
    UUID clearListenerId = productMap.addEntryListener(clearedListener, true);
    UUID evictListenerId = productMap.addEntryListener(evictedListener, true);

    System.out.println("已注册单独事件监听器");

    // 执行各种操作
    Product product = new Product(5L, "相机", "电子产品", new BigDecimal("2599.00"), 50);
    productMap.put(product.getId(), product);

    product.setPrice(new BigDecimal("2499.00"));
    productMap.put(product.getId(), product);

    productMap.remove(product.getId());

    try {
      Thread.sleep(1000);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }

    // 移除监听器
    productMap.removeEntryListener(addListenerId);
    productMap.removeEntryListener(updateListenerId);
    productMap.removeEntryListener(removeListenerId);
    productMap.removeEntryListener(clearListenerId);
    productMap.removeEntryListener(evictListenerId);
  }

  /**
   * 特定键监听器示例
   */
  public void keyListenerDemo() {
    System.out.println("\n--- 特定键监听器示例 ---");

    IMap<Long, Product> productMap = hazelcastInstance.getMap(MAP_NAME);

    // 准备特定键的数据
    Long specificKey = 5L;
    Product keyProduct = new Product(specificKey, "耳机", "配件", new BigDecimal("299.00"), 300);
    productMap.put(specificKey, keyProduct);

    // 注册针对特定键的监听器
    EntryListenerAdapter keyListener = new EntryListenerAdapter("特定键监听器");
    UUID keyListenerId = productMap.addEntryListener(keyListener, specificKey, true);
    System.out.println("已注册单键监听器，ID: " + keyListenerId);

    // 更新特定键
    keyProduct.setPrice(new BigDecimal("279.00"));
    productMap.put(specificKey, keyProduct);

    // 更新其他键（不应触发监听器）
    Product otherProduct = new Product(6L, "鼠标", "配件", new BigDecimal("129.00"), 200);
    productMap.put(otherProduct.getId(), otherProduct);

    try {
      Thread.sleep(1000);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }

    // 移除监听器
    boolean removed = productMap.removeEntryListener(keyListenerId);
    System.out.println("单键监听器移除结果: " + removed);
  }

  /**
   * 结构变化监听器示例
   */
  public void structuralChangesListenerDemo() {
    System.out.println("\n--- 结构变化监听器示例 ---");

    IMap<Long, Product> productMap = hazelcastInstance.getMap(MAP_NAME);

    // 添加分区丢失监听器
    com.hazelcast.map.listener.MapPartitionLostListener partitionLostListener = event -> {
      System.out.println("[分区丢失监听器] 分区丢失事件: " + event);
      System.out.println("  分区ID: " + event.getPartitionId());
    };

    // 注册分区监听器
    UUID partitionListenerId = productMap.addPartitionLostListener(partitionLostListener);
    System.out.println("已注册分区丢失监听器，ID: " + partitionListenerId);

    System.out.println("注意: 分区丢失事件在正常操作中不会触发，这里只是展示注册方式");

    // 移除分区监听器
    boolean removed = productMap.removePartitionLostListener(partitionListenerId);
    System.out.println("分区丢失监听器移除结果: " + removed);
  }

  /**
   * 临时监听器示例
   */
  public void temporaryListenerDemo() {
    System.out.println("\n--- 临时监听器示例 ---");

    IMap<Long, Product> productMap = hazelcastInstance.getMap(MAP_NAME);

    // 创建同步等待工具
    CountDownLatch latch = new CountDownLatch(1);

    // 注册临时监听器
    UUID tempListenerId = productMap.addEntryListener(new EntryListenerAdapter("临时监听器") {
      @Override
      public void entryAdded(EntryEvent<Long, Product> event) {
        super.entryAdded(event);
        latch.countDown(); // 减少计数，释放等待
      }
    }, true);

    System.out.println("已注册临时监听器，等待添加事件...");

    // 在另一个线程中执行添加操作
    new Thread(() -> {
      try {
        Thread.sleep(1000); // 等待1秒再添加
        System.out.println("在另一个线程中添加产品...");
        Product product = new Product(7L, "显示器", "电子产品", new BigDecimal("1499.00"), 100);
        productMap.put(product.getId(), product);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }).start();

    // 等待事件或超时
    try {
      boolean eventReceived = latch.await(5, TimeUnit.SECONDS);
      System.out.println("等待结果: " + (eventReceived ? "收到事件" : "等待超时"));
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    } finally {
      // 移除临时监听器
      boolean removed = productMap.removeEntryListener(tempListenerId);
      System.out.println("临时监听器移除结果: " + removed);
    }
  }

  /**
   * 监听器适配器类
   */
  private static class EntryListenerAdapter implements
      EntryAddedListener<Long, Product>,
      EntryUpdatedListener<Long, Product>,
      EntryRemovedListener<Long, Product>,
      MapClearedListener,
      MapEvictedListener {

    private final String name;

    public EntryListenerAdapter() {
      this("默认监听器");
    }

    public EntryListenerAdapter(String name) {
      this.name = name;
    }

    @Override
    public void entryAdded(EntryEvent<Long, Product> event) {
      System.out.println("[" + name + "] 添加事件: " + formatEvent(event));
    }

    @Override
    public void entryUpdated(EntryEvent<Long, Product> event) {
      System.out.println("[" + name + "] 更新事件: " + formatEvent(event));
    }

    @Override
    public void entryRemoved(EntryEvent<Long, Product> event) {
      System.out.println("[" + name + "] 删除事件: " + formatEvent(event));
    }

    @Override
    public void mapCleared(MapEvent event) {
      System.out.println("[" + name + "] 清空事件: Map被清空, 影响的条目数: " + event.getNumberOfEntriesAffected());
    }

    @Override
    public void mapEvicted(MapEvent event) {
      System.out.println("[" + name + "] 淘汰事件: Map被淘汰, 影响的条目数: " + event.getNumberOfEntriesAffected());
    }

    private String formatEvent(EntryEvent<Long, Product> event) {
      return String.format(
          "键=%s, 旧值=%s, 新值=%s, 合并值=%s, 事件名称=%s",
          event.getKey(),
          event.getOldValue(),
          event.getValue(),
          event.getMergingValue(),
          event.getName());
    }
  }
}