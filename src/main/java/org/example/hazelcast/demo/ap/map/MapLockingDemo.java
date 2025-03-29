package org.example.hazelcast.demo.ap.map;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import org.example.hazelcast.demo.model.Product;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Hazelcast Map 锁定和同步操作示例
 */
@Component
public class MapLockingDemo {

  private final HazelcastInstance hazelcastInstance;
  private final String MAP_NAME = "products";

  @Autowired
  public MapLockingDemo(HazelcastInstance hazelcastInstance) {
    this.hazelcastInstance = hazelcastInstance;
  }

  /**
   * 运行所有示例
   */
  public void runAllExamples() {
    System.out.println("=================== Hazelcast Map 锁定和同步示例 ===================");
    prepareData();
    basicLockingDemo();
    tryLockDemo();
    pesimisticUpdateDemo();
    optimisticUpdateDemo();
    deadlockPreventionDemo();
  }

  /**
   * 准备示例数据
   */
  private void prepareData() {
    System.out.println("\n--- 准备示例数据 ---");

    IMap<Long, Product> productMap = hazelcastInstance.getMap(MAP_NAME);
    productMap.clear();

    // 添加产品数据
    productMap.put(1L, new Product(1L, "笔记本电脑", "电子产品", new BigDecimal("5999.00"), 100));
    productMap.put(2L, new Product(2L, "智能手机", "电子产品", new BigDecimal("3999.00"), 200));

    System.out.println("已添加2个产品示例");
  }

  /**
   * 基本锁定示例
   */
  public void basicLockingDemo() {
    System.out.println("\n--- 基本锁定示例 ---");

    IMap<Long, Product> productMap = hazelcastInstance.getMap(MAP_NAME);
    Long productId = 1L;

    System.out.println("锁定前产品: " + productMap.get(productId));

    // 对条目执行锁定
    productMap.lock(productId);
    try {
      System.out.println("已锁定产品ID: " + productId);

      // 在锁定状态下更新产品
      Product product = productMap.get(productId);
      // 减少库存
      product.setStock(product.getStock() - 10);
      // 更新产品
      productMap.put(productId, product);

      System.out.println("已在锁定状态下更新产品");

      // 模拟一些处理时间
      Thread.sleep(1000);

      System.out.println("锁定后产品: " + productMap.get(productId));
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    } finally {
      // 释放锁
      productMap.unlock(productId);
      System.out.println("已释放锁");
    }
  }

  /**
   * tryLock示例
   */
  public void tryLockDemo() {
    System.out.println("\n--- tryLock示例 ---");

    IMap<Long, Product> productMap = hazelcastInstance.getMap(MAP_NAME);
    Long productId = 2L;

    // 创建锁定完成的信号
    CountDownLatch lockReleasedLatch = new CountDownLatch(1);

    // 在另一个线程中锁定产品
    new Thread(() -> {
      System.out.println("线程1: 尝试锁定产品ID: " + productId);
      productMap.lock(productId);
      try {
        System.out.println("线程1: 已锁定产品ID: " + productId + "，持有3秒");
        Thread.sleep(3000);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      } finally {
        productMap.unlock(productId);
        System.out.println("线程1: 已释放锁");
        lockReleasedLatch.countDown();
      }
    }).start();

    // 给线程1一些时间获取锁
    try {
      Thread.sleep(500);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }

    // 在主线程中尝试锁定同一个产品
    System.out.println("主线程: 尝试锁定产品ID: " + productId + " (等待1秒)");
    try {
      boolean locked = productMap.tryLock(productId, 1, TimeUnit.SECONDS);
      if (locked) {
        try {
          System.out.println("主线程: 成功获取锁！");

          // 在锁定状态下更新产品
          Product product = productMap.get(productId);
          product.setStock(product.getStock() - 5);
          productMap.put(productId, product);

          System.out.println("主线程: 已更新产品");
        } finally {
          productMap.unlock(productId);
          System.out.println("主线程: 已释放锁");
        }
      } else {
        System.out.println("主线程: 无法获取锁，等待超时");

        // 等待线程1释放锁
        lockReleasedLatch.await(5, TimeUnit.SECONDS);

        System.out.println("主线程: 线程1已释放锁，再次尝试获取锁");

        // 再次尝试获取锁
        if (productMap.tryLock(productId)) {
          try {
            System.out.println("主线程: 成功获取锁！");

            // 在锁定状态下更新产品
            Product product = productMap.get(productId);
            product.setStock(product.getStock() - 5);
            productMap.put(productId, product);

            System.out.println("主线程: 已更新产品");
          } finally {
            productMap.unlock(productId);
            System.out.println("主线程: 已释放锁");
          }
        }
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  /**
   * 悲观更新示例
   */
  public void pesimisticUpdateDemo() {
    System.out.println("\n--- 悲观更新示例 ---");

    IMap<Long, Product> productMap = hazelcastInstance.getMap(MAP_NAME);
    Long productId = 1L;

    System.out.println("更新前产品: " + productMap.get(productId));

    // 使用悲观锁定进行更新
    productMap.lock(productId);
    try {
      // 获取当前产品
      Product product = productMap.get(productId);
      if (product != null) {
        // 降低价格
        BigDecimal currentPrice = product.getPrice();
        BigDecimal newPrice = currentPrice.multiply(new BigDecimal("0.9")).setScale(2, BigDecimal.ROUND_HALF_UP);
        product.setPrice(newPrice);

        // 更新产品
        productMap.put(productId, product);

        System.out.println("已悲观更新产品价格从 " + currentPrice + " 到 " + newPrice);
      }
    } finally {
      productMap.unlock(productId);
    }

    System.out.println("更新后产品: " + productMap.get(productId));
  }

  /**
   * 乐观更新示例
   */
  public void optimisticUpdateDemo() {
    System.out.println("\n--- 乐观更新示例 ---");

    IMap<Long, Product> productMap = hazelcastInstance.getMap(MAP_NAME);
    Long productId = 2L;

    System.out.println("更新前产品: " + productMap.get(productId));

    // 使用乐观方式进行更新 (使用replace方法)
    boolean updated = false;
    int attempts = 0;
    final int MAX_ATTEMPTS = 3;

    while (!updated && attempts < MAX_ATTEMPTS) {
      attempts++;

      // 获取当前产品
      Product currentProduct = productMap.get(productId);
      if (currentProduct == null) {
        System.out.println("产品不存在");
        break;
      }

      // 创建更新后的产品副本
      Product updatedProduct = new Product(
          currentProduct.getId(),
          currentProduct.getName(),
          currentProduct.getCategory(),
          currentProduct.getPrice(),
          currentProduct.getStock() - 20 // 减少库存
      );

      // 尝试替换，只有当当前值匹配时才会成功
      updated = productMap.replace(productId, currentProduct, updatedProduct);

      System.out.println("尝试 #" + attempts + ": " + (updated ? "成功" : "失败"));

      if (!updated) {
        // 在实际场景中，可能会加入一些随机延迟以减少冲突
        try {
          Thread.sleep(100);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          break;
        }
      }
    }

    System.out.println("乐观更新" + (updated ? "成功" : "失败") + "，尝试次数: " + attempts);
    System.out.println("更新后产品: " + productMap.get(productId));
  }

  /**
   * 防止死锁示例
   */
  public void deadlockPreventionDemo() {
    System.out.println("\n--- 防止死锁示例 ---");

    IMap<Long, Product> productMap = hazelcastInstance.getMap(MAP_NAME);
    Long productId1 = 1L;
    Long productId2 = 2L;

    // 设置一个合理的锁定超时时间以防止死锁
    final long LOCK_TIMEOUT_SECONDS = 5;

    System.out.println("尝试锁定多个产品，使用超时防止死锁");

    boolean allLocked = false;
    try {
      // 尝试锁定第一个产品
      boolean locked1 = productMap.tryLock(productId1, LOCK_TIMEOUT_SECONDS, TimeUnit.SECONDS);
      if (locked1) {
        try {
          System.out.println("成功锁定产品1");

          // 尝试锁定第二个产品
          boolean locked2 = productMap.tryLock(productId2, LOCK_TIMEOUT_SECONDS, TimeUnit.SECONDS);
          if (locked2) {
            try {
              System.out.println("成功锁定产品2");
              allLocked = true;

              // 现在可以安全地更新两个产品
              Product product1 = productMap.get(productId1);
              Product product2 = productMap.get(productId2);

              // 交换产品库存
              int temp = product1.getStock();
              product1.setStock(product2.getStock());
              product2.setStock(temp);

              // 更新产品
              productMap.put(productId1, product1);
              productMap.put(productId2, product2);

              System.out.println("已交换产品库存");
              System.out.println("产品1库存: " + product1.getStock());
              System.out.println("产品2库存: " + product2.getStock());
            } finally {
              productMap.unlock(productId2);
              System.out.println("已释放产品2锁");
            }
          } else {
            System.out.println("无法在超时时间内锁定产品2，放弃操作");
          }
        } finally {
          productMap.unlock(productId1);
          System.out.println("已释放产品1锁");
        }
      } else {
        System.out.println("无法在超时时间内锁定产品1，放弃操作");
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      System.out.println("操作被中断");
    }

    System.out.println("锁定和更新操作" + (allLocked ? "成功" : "失败"));
  }
}