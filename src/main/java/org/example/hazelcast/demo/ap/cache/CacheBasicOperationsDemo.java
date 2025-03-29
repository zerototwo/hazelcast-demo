package org.example.hazelcast.demo.ap.cache;

import com.hazelcast.cache.ICache;
import com.hazelcast.core.HazelcastInstance;
import org.example.hazelcast.demo.model.Product;
import org.springframework.stereotype.Component;

import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.Caching;
import javax.cache.configuration.CompleteConfiguration;
import javax.cache.configuration.FactoryBuilder;
import javax.cache.configuration.MutableConfiguration;
import javax.cache.expiry.CreatedExpiryPolicy;
import javax.cache.expiry.Duration;
import javax.cache.expiry.ExpiryPolicy;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import javax.cache.spi.CachingProvider;
import java.math.BigDecimal;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

/**
 * Hazelcast Cache 基本操作示例
 * 
 * 官方定义：
 * Hazelcast Cache是JCache(JSR-107)规范的分布式实现，提供分布式缓存功能。
 * JCache是Java缓存的标准API，它定义了一组通用接口来缓存对象。
 * 
 * 主要特性：
 * 1. 标准兼容：完全实现JCache(JSR-107)规范
 * 2. 分布式存储：数据分布在集群的所有成员之间
 * 3. 近实时缓存：提供高速读写和数据一致性
 * 4. 过期策略：支持基于时间的缓存项过期
 * 5. 存储媒介：支持内存存储和持久化存储
 * 6. 事件监听：支持缓存更改事件的监听
 * 7. 近数据处理：通过EntryProcessor在数据所在节点执行操作
 * 8. 缓存加载：支持同步和异步数据加载
 * 9. 原子操作：提供多种原子操作保证数据一致性
 * 
 * 基础架构组件：
 * - CachingProvider：管理CacheManager生命周期
 * - CacheManager：管理和控制多个命名缓存
 * - Cache：存储数据的主要接口
 * - Entry：缓存中的单个键值对
 * - ExpiryPolicy：定义缓存项的过期策略
 * - CacheLoader：定义如何从外部源加载数据
 * - CacheWriter：定义如何将数据写入外部源
 * 
 * 适用场景：
 * - 高性能数据访问
 * - 分布式应用加速
 * - 数据库请求减负
 * - Web应用会话存储
 * - 高频读取低频写入数据
 * - 临时数据存储
 * - 预计算结果缓存
 * 
 * Hazelcast Cache与其他数据结构的区别：
 * - 与Map区别：Cache遵循JSR-107标准，提供更丰富的缓存功能，如过期策略
 * - 与Redis/Memcached区别：是Java特定的，集成到JVM中，并支持集群
 * - 与本地缓存区别：提供分布式功能、持久性和容错能力
 * 
 * 缓存策略：
 * - 读取策略：Read-Through、Cache-Aside
 * - 写入策略：Write-Through、Write-Behind、Write-Around
 * - 过期策略：访问后过期、创建后过期、更新后过期
 * 
 * 本示例演示了Hazelcast Cache的基本操作，包括缓存的创建、
 * 数据操作、过期策略设置、批量操作等功能。
 */
@Component
public class CacheBasicOperationsDemo {

  private final String CACHE_NAME = "products";
  private CacheManager cacheManager;
  private CachingProvider cachingProvider;
  private final HazelcastInstance hazelcastInstance;

  public CacheBasicOperationsDemo(HazelcastInstance hazelcastInstance) {
    this.hazelcastInstance = hazelcastInstance;
  }

  /**
   * 运行所有基本操作示例
   */
  public void runAllExamples() {
    System.out.println("=================== Hazelcast JCache 基本操作示例 ===================");

    initCacheManager();
    createCacheExample();
    putExample();
    getExample();
    containsKeyExample();
    updateExample();
    removeExample();
    destroyExample();

    closeCacheManager();
  }

  /**
   * 初始化缓存管理器
   */
  private void initCacheManager() {
    System.out.println("\n--- 初始化 JCache CacheManager ---");

    // 获取JCache提供者
    cachingProvider = Caching.getCachingProvider();
    System.out.println("JCache提供者: " + cachingProvider.getClass().getName());

    // 获取缓存管理器
    cacheManager = cachingProvider.getCacheManager();
    System.out.println("CacheManager初始化成功");
  }

  /**
   * 创建缓存示例
   */
  public void createCacheExample() {
    System.out.println("\n--- 创建缓存示例 ---");

    // 检查缓存是否已存在
    if (cacheManager.getCache(CACHE_NAME, Long.class, Product.class) != null) {
      System.out.println("缓存 '" + CACHE_NAME + "' 已存在，将被关闭");
      cacheManager.destroyCache(CACHE_NAME);
    }

    // 创建基本缓存配置
    MutableConfiguration<Long, Product> config = new MutableConfiguration<Long, Product>()
        .setTypes(Long.class, Product.class)
        .setStoreByValue(true)
        .setStatisticsEnabled(true);

    // 创建缓存
    Cache<Long, Product> cache = cacheManager.createCache(CACHE_NAME, config);
    System.out.println("创建缓存: " + CACHE_NAME);

    // 获取Hazelcast的ICache扩展
    ICache<Long, Product> iCache = cache.unwrap(ICache.class);
    System.out.println("成功获取ICache实例");
  }

  /**
   * 添加条目示例
   */
  public void putExample() {
    System.out.println("\n--- Cache.put() 方法示例 ---");

    // 获取缓存
    Cache<Long, Product> cache = cacheManager.getCache(CACHE_NAME, Long.class, Product.class);

    // 添加条目
    Product product1 = new Product(1L, "笔记本电脑", "电子产品", new BigDecimal("5999.00"), 100);
    cache.put(product1.getId(), product1);
    System.out.println("添加了产品: " + product1);

    // 添加更多条目
    Product product2 = new Product(2L, "智能手机", "电子产品", new BigDecimal("3999.00"), 200);
    cache.put(product2.getId(), product2);
    System.out.println("添加了产品: " + product2);

    // 使用Hazelcast的ICache扩展功能 - 设置有过期时间的条目
    ICache<Long, Product> iCache = cache.unwrap(ICache.class);
    Product product3 = new Product(3L, "耳机", "配件", new BigDecimal("299.00"), 500);
    iCache.put(product3.getId(), product3, CreatedExpiryPolicy.factoryOf(new Duration(TimeUnit.SECONDS, 30)).create());
    System.out.println("添加了具有30秒过期时间的产品: " + product3);

    // putIfAbsent - 仅在键不存在时添加
    Product product4 = new Product(4L, "平板电脑", "电子产品", new BigDecimal("2999.00"), 150);
    boolean wasAbsent = cache.putIfAbsent(product4.getId(), product4);
    System.out.println("putIfAbsent: " + (wasAbsent ? "添加成功" : "已存在，添加失败"));

    // 再次尝试putIfAbsent
    Product product4Duplicate = new Product(4L, "平板电脑Pro", "电子产品", new BigDecimal("3999.00"), 150);
    wasAbsent = cache.putIfAbsent(product4.getId(), product4Duplicate);
    System.out.println("再次putIfAbsent: " + (wasAbsent ? "添加成功" : "已存在，添加失败"));
  }

  /**
   * 获取条目示例
   */
  public void getExample() {
    System.out.println("\n--- Cache.get() 方法示例 ---");

    Cache<Long, Product> cache = cacheManager.getCache(CACHE_NAME, Long.class, Product.class);

    // 获取条目
    Product product = cache.get(1L);
    System.out.println("获取ID为1的产品: " + product);

    // 获取不存在的条目
    Product nonExistent = cache.get(999L);
    System.out.println("获取不存在的产品: " + nonExistent);

    // 遍历所有条目
    System.out.println("所有缓存条目:");
    Iterator<Cache.Entry<Long, Product>> iterator = cache.iterator();
    while (iterator.hasNext()) {
      Cache.Entry<Long, Product> entry = iterator.next();
      System.out.println(" - 键: " + entry.getKey() + ", 值: " + entry.getValue());
    }
  }

  /**
   * 包含键检查示例
   */
  public void containsKeyExample() {
    System.out.println("\n--- Cache.containsKey() 方法示例 ---");

    Cache<Long, Product> cache = cacheManager.getCache(CACHE_NAME, Long.class, Product.class);

    // 检查键是否存在
    boolean hasKey1 = cache.containsKey(1L);
    System.out.println("缓存包含键1: " + hasKey1);

    boolean hasKey999 = cache.containsKey(999L);
    System.out.println("缓存包含键999: " + hasKey999);
  }

  /**
   * 更新条目示例
   */
  public void updateExample() {
    System.out.println("\n--- 缓存更新操作示例 ---");

    Cache<Long, Product> cache = cacheManager.getCache(CACHE_NAME, Long.class, Product.class);

    // 直接更新
    Product updatedProduct = new Product(1L, "高性能笔记本电脑", "电子产品", new BigDecimal("7999.00"), 50);
    cache.put(updatedProduct.getId(), updatedProduct);
    System.out.println("更新后的产品: " + cache.get(1L));

    // Hazelcast ICache的replace操作
    ICache<Long, Product> iCache = cache.unwrap(ICache.class);
    Product oldProduct = cache.get(2L);
    Product newProduct = new Product(2L, "旗舰智能手机", "电子产品", new BigDecimal("4999.00"), 100);
    boolean replaced = iCache.replace(oldProduct.getId(), oldProduct, newProduct);
    System.out.println("使用replace(key, oldValue, newValue)更新结果: " + replaced);
    System.out.println("更新后的产品: " + cache.get(2L));

    // 异步更新 (Hazelcast扩展)
    iCache.putAsync(3L, new Product(3L, "无线耳机", "配件", new BigDecimal("399.00"), 300))
        .thenAccept(oldValue -> System.out.println("异步更新成功，旧值: " + oldValue));
  }

  /**
   * 删除条目示例
   */
  public void removeExample() {
    System.out.println("\n--- 缓存删除操作示例 ---");

    Cache<Long, Product> cache = cacheManager.getCache(CACHE_NAME, Long.class, Product.class);
    ICache<Long, Product> iCache = cache.unwrap(ICache.class);

    // 输出当前大小 (使用Hazelcast的ICache扩展)
    System.out.println("删除前缓存大小: " + iCache.size());

    // 简单删除
    boolean removed = cache.remove(3L);
    System.out.println("删除ID为3的产品: " + removed);

    // 条件删除
    Product product4 = cache.get(4L);
    boolean removed2 = cache.remove(4L, product4);
    System.out.println("条件删除结果: " + removed2);

    // 异步删除 (Hazelcast扩展)
    iCache.removeAsync(1L)
        .thenAccept(wasRemoved -> System.out.println("异步删除结果: " + wasRemoved));

    // 输出当前大小
    // 注意：由于异步操作，这里可能不会立即反映所有删除
    System.out.println("删除后缓存大小: " + iCache.size());
  }

  /**
   * 销毁缓存示例
   */
  public void destroyExample() {
    System.out.println("\n--- 销毁缓存示例 ---");

    // 检查缓存是否存在
    if (cacheManager.getCache(CACHE_NAME, Long.class, Product.class) != null) {
      // 销毁缓存
      cacheManager.destroyCache(CACHE_NAME);
      System.out.println("缓存 '" + CACHE_NAME + "' 已销毁");
    } else {
      System.out.println("缓存 '" + CACHE_NAME + "' 不存在，无需销毁");
    }
  }

  /**
   * 关闭缓存管理器
   */
  private void closeCacheManager() {
    System.out.println("\n--- 关闭 JCache CacheManager ---");

    if (cachingProvider != null) {
      cachingProvider.close();
      System.out.println("CachingProvider已关闭");
    }
  }
}