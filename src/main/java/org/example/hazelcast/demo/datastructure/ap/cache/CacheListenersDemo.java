package org.example.hazelcast.demo.datastructure.ap.cache;

import com.hazelcast.cache.ICache;
import org.example.hazelcast.demo.model.Product;
import org.springframework.stereotype.Component;

import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.Caching;
import javax.cache.configuration.CacheEntryListenerConfiguration;
import javax.cache.configuration.FactoryBuilder;
import javax.cache.configuration.MutableCacheEntryListenerConfiguration;
import javax.cache.configuration.MutableConfiguration;
import javax.cache.event.*;
import javax.cache.spi.CachingProvider;
import java.io.Serializable;
import java.math.BigDecimal;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * JCache 监听器示例
 */
@Component
public class CacheListenersDemo {

  private final String CACHE_NAME = "products";
  private CachingProvider cachingProvider;
  private CacheManager cacheManager;

  /**
   * 运行所有监听器示例
   */
  public void runAllExamples() {
    System.out.println("=================== Hazelcast JCache 监听器示例 ===================");

    initCacheManager();
    basicListenerExample();
    filteringListenerExample();
    synchronousListenerExample();
    oldValueListenerExample();
    multipleListenersExample();

    closeCacheManager();
  }

  /**
   * 初始化缓存管理器
   */
  private void initCacheManager() {
    System.out.println("\n--- 初始化 JCache CacheManager ---");

    // 获取JCache提供者
    cachingProvider = Caching.getCachingProvider();

    // 获取缓存管理器
    cacheManager = cachingProvider.getCacheManager();

    // 如果缓存已存在，销毁它
    if (cacheManager.getCache(CACHE_NAME) != null) {
      cacheManager.destroyCache(CACHE_NAME);
    }
  }

  /**
   * 基本监听器示例
   */
  public void basicListenerExample() {
    System.out.println("\n--- 基本缓存监听器示例 ---");

    // 创建一个简单的监听器工厂
    SimpleCacheEntryListener<Long, Product> listener = new SimpleCacheEntryListener<>();

    // 创建监听器配置
    CacheEntryListenerConfiguration<Long, Product> listenerConfig = new MutableCacheEntryListenerConfiguration<>(
        FactoryBuilder.factoryOf(listener), // 监听器工厂
        null, // 过滤器工厂
        false, // 是否为同步
        false); // 是否接收旧值

    // 创建缓存配置并添加监听器
    MutableConfiguration<Long, Product> config = new MutableConfiguration<Long, Product>()
        .setTypes(Long.class, Product.class)
        .addCacheEntryListenerConfiguration(listenerConfig);

    // 创建缓存
    Cache<Long, Product> cache = cacheManager.createCache(CACHE_NAME, config);
    System.out.println("已创建带有监听器的缓存");

    // 执行一些操作来触发监听器
    System.out.println("执行缓存操作来触发监听器...");

    // 添加
    Product product1 = new Product(1L, "笔记本电脑", "电子产品", new BigDecimal("5999.00"), 100);
    cache.put(product1.getId(), product1);

    // 更新
    Product updatedProduct = new Product(1L, "高性能笔记本电脑", "电子产品", new BigDecimal("7999.00"), 80);
    cache.put(updatedProduct.getId(), updatedProduct);

    // 删除
    cache.remove(1L);

    System.out.println("监听器收到的事件: " + listener.getReceivedEvents());

    // 清理
    cacheManager.destroyCache(CACHE_NAME);
  }

  /**
   * 带过滤器的监听器示例
   */
  public void filteringListenerExample() {
    System.out.println("\n--- 带过滤器的监听器示例 ---");

    // 创建监听器
    SimpleCacheEntryListener<Long, Product> listener = new SimpleCacheEntryListener<>();

    // 创建过滤器 - 只关注电子产品类别
    ProductCategoryFilter filter = new ProductCategoryFilter("电子产品");

    // 创建监听器配置
    CacheEntryListenerConfiguration<Long, Product> listenerConfig = new MutableCacheEntryListenerConfiguration<>(
        FactoryBuilder.factoryOf(listener),
        FactoryBuilder.factoryOf(filter), // 使用我们的过滤器
        false,
        false);

    // 创建缓存配置并添加监听器
    MutableConfiguration<Long, Product> config = new MutableConfiguration<Long, Product>()
        .setTypes(Long.class, Product.class)
        .addCacheEntryListenerConfiguration(listenerConfig);

    // 创建缓存
    Cache<Long, Product> cache = cacheManager.createCache(CACHE_NAME, config);
    System.out.println("已创建带有过滤器监听器的缓存");

    // 添加几个不同类别的产品
    System.out.println("添加不同类别的产品...");
    cache.put(1L, new Product(1L, "笔记本电脑", "电子产品", new BigDecimal("5999.00"), 100));
    cache.put(2L, new Product(2L, "办公椅", "家具", new BigDecimal("899.00"), 50));
    cache.put(3L, new Product(3L, "智能手机", "电子产品", new BigDecimal("3999.00"), 200));
    cache.put(4L, new Product(4L, "书桌", "家具", new BigDecimal("1299.00"), 30));

    // 更新和删除一些条目
    cache.put(1L, new Product(1L, "高端笔记本电脑", "电子产品", new BigDecimal("7999.00"), 80));
    cache.remove(2L); // 家具类别应该被过滤掉
    cache.remove(3L); // 电子产品类别应该被监听到

    System.out.println("监听器收到的事件: " + listener.getReceivedEvents());
    System.out.println("注意：只有电子产品类别的事件被监听到");

    // 清理
    cacheManager.destroyCache(CACHE_NAME);
  }

  /**
   * 同步监听器示例
   */
  public void synchronousListenerExample() {
    System.out.println("\n--- 同步监听器示例 ---");

    // 创建一个会延迟处理的监听器
    DelayingCacheEntryListener<Long, Product> listener = new DelayingCacheEntryListener<>(500);

    // 创建监听器配置，设置为同步
    CacheEntryListenerConfiguration<Long, Product> listenerConfig = new MutableCacheEntryListenerConfiguration<>(
        FactoryBuilder.factoryOf(listener),
        null,
        true, // 同步模式
        false);

    // 创建缓存配置并添加监听器
    MutableConfiguration<Long, Product> config = new MutableConfiguration<Long, Product>()
        .setTypes(Long.class, Product.class)
        .addCacheEntryListenerConfiguration(listenerConfig);

    // 创建缓存
    Cache<Long, Product> cache = cacheManager.createCache(CACHE_NAME, config);
    System.out.println("已创建带有同步监听器的缓存");

    // 执行操作并测量时间
    System.out.println("执行缓存操作并测量时间...");

    long startTime = System.currentTimeMillis();
    cache.put(1L, new Product(1L, "笔记本电脑", "电子产品", new BigDecimal("5999.00"), 100));
    long endTime = System.currentTimeMillis();

    System.out.println("同步put操作耗时: " + (endTime - startTime) + " ms");
    System.out.println("注意：操作会等待监听器处理完成后才返回");

    // 清理
    cacheManager.destroyCache(CACHE_NAME);
  }

  /**
   * 接收旧值的监听器示例
   */
  public void oldValueListenerExample() {
    System.out.println("\n--- 接收旧值的监听器示例 ---");

    // 创建一个接收旧值的监听器
    OldValueAwareCacheEntryListener<Long, Product> listener = new OldValueAwareCacheEntryListener<>();

    // 创建监听器配置，设置为接收旧值
    CacheEntryListenerConfiguration<Long, Product> listenerConfig = new MutableCacheEntryListenerConfiguration<>(
        FactoryBuilder.factoryOf(listener),
        null,
        false,
        true); // 接收旧值

    // 创建缓存配置并添加监听器
    MutableConfiguration<Long, Product> config = new MutableConfiguration<Long, Product>()
        .setTypes(Long.class, Product.class)
        .addCacheEntryListenerConfiguration(listenerConfig);

    // 创建缓存
    Cache<Long, Product> cache = cacheManager.createCache(CACHE_NAME, config);
    System.out.println("已创建带有接收旧值的监听器的缓存");

    // 添加和更新条目
    Product product1 = new Product(1L, "笔记本电脑", "电子产品", new BigDecimal("5999.00"), 100);
    cache.put(product1.getId(), product1);

    Product updatedProduct = new Product(1L, "高性能笔记本电脑", "电子产品", new BigDecimal("7999.00"), 80);
    cache.put(updatedProduct.getId(), updatedProduct);

    System.out.println("监听器收到的更新事件: " + listener.getUpdates());

    // 清理
    cacheManager.destroyCache(CACHE_NAME);
  }

  /**
   * 多个监听器示例
   */
  public void multipleListenersExample() {
    System.out.println("\n--- 多个监听器示例 ---");

    // 创建一个只关注创建事件的监听器
    CreatedOnlyListener<Long, Product> createdListener = new CreatedOnlyListener<>();
    CacheEntryListenerConfiguration<Long, Product> createdListenerConfig = new MutableCacheEntryListenerConfiguration<>(
        FactoryBuilder.factoryOf(createdListener),
        null,
        false,
        false);

    // 创建一个只关注删除事件的监听器
    RemovedOnlyListener<Long, Product> removedListener = new RemovedOnlyListener<>();
    CacheEntryListenerConfiguration<Long, Product> removedListenerConfig = new MutableCacheEntryListenerConfiguration<>(
        FactoryBuilder.factoryOf(removedListener),
        null,
        false,
        false);

    // 创建缓存配置并添加两个监听器
    MutableConfiguration<Long, Product> config = new MutableConfiguration<Long, Product>()
        .setTypes(Long.class, Product.class)
        .addCacheEntryListenerConfiguration(createdListenerConfig)
        .addCacheEntryListenerConfiguration(removedListenerConfig);

    // 创建缓存
    Cache<Long, Product> cache = cacheManager.createCache(CACHE_NAME, config);
    System.out.println("已创建带有多个监听器的缓存");

    // 执行不同类型的操作
    cache.put(1L, new Product(1L, "笔记本电脑", "电子产品", new BigDecimal("5999.00"), 100));
    cache.put(2L, new Product(2L, "智能手机", "电子产品", new BigDecimal("3999.00"), 200));
    cache.put(1L, new Product(1L, "高端笔记本电脑", "电子产品", new BigDecimal("7999.00"), 80)); // 更新
    cache.remove(2L);

    System.out.println("创建监听器收到的事件数: " + createdListener.getCreatedCount());
    System.out.println("删除监听器收到的事件数: " + removedListener.getRemovedCount());

    // 动态注册新的监听器
    System.out.println("\n动态注册新的监听器...");

    // 创建一个只关注更新事件的监听器
    UpdatedOnlyListener<Long, Product> updatedListener = new UpdatedOnlyListener<>();
    CacheEntryListenerConfiguration<Long, Product> updatedListenerConfig = new MutableCacheEntryListenerConfiguration<>(
        FactoryBuilder.factoryOf(updatedListener),
        null,
        false,
        false);

    // 动态注册
    cache.registerCacheEntryListener(updatedListenerConfig);

    // 再次执行一些操作
    cache.put(3L, new Product(3L, "平板电脑", "电子产品", new BigDecimal("2999.00"), 150));
    cache.put(3L, new Product(3L, "高端平板电脑", "电子产品", new BigDecimal("3999.00"), 120)); // 更新
    cache.remove(1L);

    System.out.println("创建监听器收到的事件数: " + createdListener.getCreatedCount());
    System.out.println("更新监听器收到的事件数: " + updatedListener.getUpdatedCount());
    System.out.println("删除监听器收到的事件数: " + removedListener.getRemovedCount());

    // 取消注册监听器
    System.out.println("\n取消注册删除监听器...");
    cache.deregisterCacheEntryListener(removedListenerConfig);

    cache.remove(3L); // 这次删除事件不会被监听到

    System.out.println("删除监听器收到的事件数 (未改变): " + removedListener.getRemovedCount());

    // 清理
    cacheManager.destroyCache(CACHE_NAME);
  }

  /**
   * 关闭缓存管理器
   */
  private void closeCacheManager() {
    if (cachingProvider != null) {
      System.out.println("\n--- 关闭 JCache CacheManager ---");
      cachingProvider.close();
    }
  }

  /**
   * 简单的缓存条目监听器，记录所有类型的事件
   */
  public static class SimpleCacheEntryListener<K, V> implements CacheEntryCreatedListener<K, V>,
      CacheEntryUpdatedListener<K, V>, CacheEntryRemovedListener<K, V>,
      CacheEntryExpiredListener<K, V>, Serializable {

    private static final long serialVersionUID = 1L;

    private int createdCount;
    private int updatedCount;
    private int removedCount;
    private int expiredCount;

    @Override
    public void onCreated(Iterable<CacheEntryEvent<? extends K, ? extends V>> events) {
      for (CacheEntryEvent<? extends K, ? extends V> event : events) {
        System.out.println("创建事件: 键=" + event.getKey() + ", 值=" + event.getValue());
        createdCount++;
      }
    }

    @Override
    public void onUpdated(Iterable<CacheEntryEvent<? extends K, ? extends V>> events) {
      for (CacheEntryEvent<? extends K, ? extends V> event : events) {
        System.out.println("更新事件: 键=" + event.getKey() + ", 值=" + event.getValue());
        updatedCount++;
      }
    }

    @Override
    public void onRemoved(Iterable<CacheEntryEvent<? extends K, ? extends V>> events) {
      for (CacheEntryEvent<? extends K, ? extends V> event : events) {
        System.out.println("删除事件: 键=" + event.getKey() + ", 值=" + event.getValue());
        removedCount++;
      }
    }

    @Override
    public void onExpired(Iterable<CacheEntryEvent<? extends K, ? extends V>> events) {
      for (CacheEntryEvent<? extends K, ? extends V> event : events) {
        System.out.println("过期事件: 键=" + event.getKey() + ", 值=" + event.getValue());
        expiredCount++;
      }
    }

    public String getReceivedEvents() {
      return "创建=" + createdCount + ", 更新=" + updatedCount +
          ", 删除=" + removedCount + ", 过期=" + expiredCount;
    }
  }

  /**
   * 产品类别过滤器，只允许特定类别的产品事件通过
   */
  public static class ProductCategoryFilter implements CacheEntryEventFilter<Long, Product>, Serializable {

    private static final long serialVersionUID = 1L;

    private final String targetCategory;

    public ProductCategoryFilter(String targetCategory) {
      this.targetCategory = targetCategory;
    }

    @Override
    public boolean evaluate(CacheEntryEvent<? extends Long, ? extends Product> event) {
      // 只关注特定类别的产品
      Product product = event.getValue();
      boolean matches = product != null && targetCategory.equals(product.getCategory());

      if (!matches) {
        System.out.println("过滤掉事件: 键=" + event.getKey() + ", 类别=" +
            (product != null ? product.getCategory() : "null"));
      }

      return matches;
    }
  }

  /**
   * 延迟处理的监听器，模拟处理需要时间的情况
   */
  public static class DelayingCacheEntryListener<K, V> implements CacheEntryCreatedListener<K, V>, Serializable {

    private static final long serialVersionUID = 1L;

    private final long delayMs;

    public DelayingCacheEntryListener(long delayMs) {
      this.delayMs = delayMs;
    }

    @Override
    public void onCreated(Iterable<CacheEntryEvent<? extends K, ? extends V>> events) {
      for (CacheEntryEvent<? extends K, ? extends V> event : events) {
        System.out.println("处理创建事件，将延迟 " + delayMs + "ms: 键=" + event.getKey());
        try {
          Thread.sleep(delayMs);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
        System.out.println("完成处理创建事件: 键=" + event.getKey());
      }
    }
  }

  /**
   * 接收旧值的监听器
   */
  public static class OldValueAwareCacheEntryListener<K, V> implements CacheEntryUpdatedListener<K, V>, Serializable {

    private static final long serialVersionUID = 1L;

    private StringBuilder updates = new StringBuilder();

    @Override
    public void onUpdated(Iterable<CacheEntryEvent<? extends K, ? extends V>> events) {
      for (CacheEntryEvent<? extends K, ? extends V> event : events) {
        String message = "更新: 键=" + event.getKey() +
            ", 旧值=" + event.getOldValue() +
            ", 新值=" + event.getValue();

        System.out.println(message);
        if (updates.length() > 0) {
          updates.append(", ");
        }
        updates.append(message);
      }
    }

    public String getUpdates() {
      return updates.toString();
    }
  }

  /**
   * 只监听创建事件的监听器
   */
  public static class CreatedOnlyListener<K, V> implements CacheEntryCreatedListener<K, V>, Serializable {

    private static final long serialVersionUID = 1L;

    private int createdCount;

    @Override
    public void onCreated(Iterable<CacheEntryEvent<? extends K, ? extends V>> events) {
      for (CacheEntryEvent<? extends K, ? extends V> event : events) {
        System.out.println("CreatedOnlyListener - 创建事件: 键=" + event.getKey());
        createdCount++;
      }
    }

    public int getCreatedCount() {
      return createdCount;
    }
  }

  /**
   * 只监听更新事件的监听器
   */
  public static class UpdatedOnlyListener<K, V> implements CacheEntryUpdatedListener<K, V>, Serializable {

    private static final long serialVersionUID = 1L;

    private int updatedCount;

    @Override
    public void onUpdated(Iterable<CacheEntryEvent<? extends K, ? extends V>> events) {
      for (CacheEntryEvent<? extends K, ? extends V> event : events) {
        System.out.println("UpdatedOnlyListener - 更新事件: 键=" + event.getKey());
        updatedCount++;
      }
    }

    public int getUpdatedCount() {
      return updatedCount;
    }
  }

  /**
   * 只监听删除事件的监听器
   */
  public static class RemovedOnlyListener<K, V> implements CacheEntryRemovedListener<K, V>, Serializable {

    private static final long serialVersionUID = 1L;

    private int removedCount;

    @Override
    public void onRemoved(Iterable<CacheEntryEvent<? extends K, ? extends V>> events) {
      for (CacheEntryEvent<? extends K, ? extends V> event : events) {
        System.out.println("RemovedOnlyListener - 删除事件: 键=" + event.getKey());
        removedCount++;
      }
    }

    public int getRemovedCount() {
      return removedCount;
    }
  }
}