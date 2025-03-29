package org.example.hazelcast.demo.ap.cache;

import com.hazelcast.cache.ICache;
import org.example.hazelcast.demo.model.Product;
import org.springframework.stereotype.Component;

import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.Caching;
import javax.cache.configuration.MutableConfiguration;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.EntryProcessorResult;
import javax.cache.processor.MutableEntry;
import javax.cache.spi.CachingProvider;
import java.io.Serializable;
import java.math.BigDecimal;
import java.util.Map;
import java.util.Set;

/**
 * JCache EntryProcessor操作示例
 */
@Component
public class CacheEntryProcessorDemo {

  private final String CACHE_NAME = "products";
  private CachingProvider cachingProvider;
  private CacheManager cacheManager;

  /**
   * 运行所有EntryProcessor操作示例
   */
  public void runAllExamples() {
    System.out.println("=================== Hazelcast JCache EntryProcessor 示例 ===================");

    initCache();
    basicEntryProcessorExample();
    multipleEntryProcessorExample();
    conditionalEntryProcessorExample();
    destructiveProcessorExample();

    closeCache();
  }

  /**
   * 初始化缓存
   */
  private void initCache() {
    // 获取JCache提供者
    cachingProvider = Caching.getCachingProvider();
    cacheManager = cachingProvider.getCacheManager();

    // 检查缓存是否已存在
    if (cacheManager.getCache(CACHE_NAME, Long.class, Product.class) != null) {
      System.out.println("缓存 '" + CACHE_NAME + "' 已存在，将被关闭");
      cacheManager.destroyCache(CACHE_NAME);
    }

    // 创建缓存配置
    MutableConfiguration<Long, Product> config = new MutableConfiguration<Long, Product>()
        .setTypes(Long.class, Product.class)
        .setStoreByValue(true)
        .setStatisticsEnabled(true);

    // 创建缓存
    Cache<Long, Product> cache = cacheManager.createCache(CACHE_NAME, config);

    // 添加测试数据
    loadTestData(cache);
  }

  /**
   * 加载测试数据
   */
  private void loadTestData(Cache<Long, Product> cache) {
    System.out.println("\n--- 加载测试数据 ---");

    cache.put(1L, new Product(1L, "笔记本电脑", "电子产品", new BigDecimal("5999.00"), 100));
    cache.put(2L, new Product(2L, "智能手机", "电子产品", new BigDecimal("3999.00"), 200));
    cache.put(3L, new Product(3L, "耳机", "配件", new BigDecimal("299.00"), 500));
    cache.put(4L, new Product(4L, "平板电脑", "电子产品", new BigDecimal("2999.00"), 150));
    cache.put(5L, new Product(5L, "智能手表", "电子产品", new BigDecimal("1999.00"), 80));

    System.out.println("已加载5个测试产品到缓存");
  }

  /**
   * 基本EntryProcessor示例
   */
  public void basicEntryProcessorExample() {
    System.out.println("\n--- 基本EntryProcessor示例 ---");

    Cache<Long, Product> cache = cacheManager.getCache(CACHE_NAME, Long.class, Product.class);

    System.out.println("处理前产品: " + cache.get(1L));

    // 创建EntryProcessor来更新价格
    EntryProcessor<Long, Product, BigDecimal> priceUpdateProcessor = new EntryProcessor<Long, Product, BigDecimal>() {
      @Override
      public BigDecimal process(MutableEntry<Long, Product> entry, Object... arguments) throws EntryProcessorException {
        if (entry.exists()) {
          Product product = entry.getValue();
          BigDecimal currentPrice = product.getPrice();
          BigDecimal newPrice = currentPrice.multiply(new BigDecimal("1.1")); // 提高10%
          product.setPrice(newPrice);
          entry.setValue(product);
          return newPrice;
        }
        return null;
      }
    };

    // 执行处理器并获取结果
    BigDecimal newPrice = cache.invoke(1L, priceUpdateProcessor);
    System.out.println("处理后产品: " + cache.get(1L));
    System.out.println("新价格: " + newPrice);
  }

  /**
   * 多条目EntryProcessor示例
   */
  public void multipleEntryProcessorExample() {
    System.out.println("\n--- 多条目EntryProcessor示例 ---");

    Cache<Long, Product> cache = cacheManager.getCache(CACHE_NAME, Long.class, Product.class);

    // 创建EntryProcessor来增加库存
    EntryProcessor<Long, Product, Integer> stockIncreaseProcessor = new EntryProcessor<Long, Product, Integer>() {
      @Override
      public Integer process(MutableEntry<Long, Product> entry, Object... arguments) throws EntryProcessorException {
        if (entry.exists()) {
          Product product = entry.getValue();
          Integer increment = (Integer) arguments[0];
          Integer oldStock = product.getStock();
          product.setStock(oldStock + increment);
          entry.setValue(product);
          return product.getStock();
        }
        return null;
      }
    };

    // 获取要处理的键集合
    Set<Long> keys = Set.of(2L, 3L, 4L);

    // 在所有产品上执行相同的processor - 增加50个库存
    System.out.println("处理前:");
    for (Long key : keys) {
      System.out.println(" - 产品 " + key + ": " + cache.get(key));
    }

    Map<Long, EntryProcessorResult<Integer>> resultMap = cache.invokeAll(keys, stockIncreaseProcessor, 50);

    System.out.println("处理后:");
    for (Long key : keys) {
      Integer newStock = resultMap.get(key).get();
      System.out.println(" - 产品 " + key + ": " + cache.get(key) + ", 新库存: " + newStock);
    }
  }

  /**
   * 条件EntryProcessor示例
   */
  public void conditionalEntryProcessorExample() {
    System.out.println("\n--- 条件EntryProcessor示例 ---");

    Cache<Long, Product> cache = cacheManager.getCache(CACHE_NAME, Long.class, Product.class);

    // 创建EntryProcessor，仅当产品属于某个类别时才处理
    EntryProcessor<Long, Product, String> categoryProcessor = new EntryProcessor<Long, Product, String>() {
      @Override
      public String process(MutableEntry<Long, Product> entry, Object... arguments) throws EntryProcessorException {
        if (entry.exists()) {
          Product product = entry.getValue();
          String targetCategory = (String) arguments[0];
          String newCategory = (String) arguments[1];

          if (product.getCategory().equals(targetCategory)) {
            product.setCategory(newCategory);
            entry.setValue(product);
            return "已更新";
          }
          return "未更新 - 类别不匹配";
        }
        return "未更新 - 条目不存在";
      }
    };

    // 获取所有键
    Set<Long> allKeys = Set.of(1L, 2L, 3L, 4L, 5L);

    System.out.println("处理前:");
    for (Long key : allKeys) {
      System.out.println(" - 产品 " + key + ": " + cache.get(key));
    }

    // 将所有"电子产品"类别更改为"高端电子产品"
    Map<Long, EntryProcessorResult<String>> resultMap = cache.invokeAll(allKeys, categoryProcessor, "电子产品", "高端电子产品");

    System.out.println("处理后:");
    for (Long key : allKeys) {
      String result = resultMap.get(key).get();
      System.out.println(" - 产品 " + key + ": " + cache.get(key) + ", 结果: " + result);
    }
  }

  /**
   * 写入或删除EntryProcessor示例
   */
  public void destructiveProcessorExample() {
    System.out.println("\n--- 写入或删除EntryProcessor示例 ---");

    Cache<Long, Product> cache = cacheManager.getCache(CACHE_NAME, Long.class, Product.class);

    // 创建EntryProcessor，删除库存低于阈值的产品
    EntryProcessor<Long, Product, String> removeIfLowStockProcessor = new RemoveIfLowStockProcessor(100);

    // 获取所有键
    Set<Long> allKeys = Set.of(1L, 2L, 3L, 4L, 5L);

    System.out.println("处理前:");
    for (Long key : allKeys) {
      Product product = cache.get(key);
      if (product != null) {
        System.out.println(" - 产品 " + key + ": " + product);
      } else {
        System.out.println(" - 产品 " + key + ": 不存在");
      }
    }

    // 执行处理器
    Map<Long, EntryProcessorResult<String>> resultMap = cache.invokeAll(allKeys, removeIfLowStockProcessor);

    System.out.println("处理后:");
    for (Long key : allKeys) {
      Product product = cache.get(key);
      String result = resultMap.get(key).get();
      if (product != null) {
        System.out.println(" - 产品 " + key + ": " + product + ", 结果: " + result);
      } else {
        System.out.println(" - 产品 " + key + ": 不存在, 结果: " + result);
      }
    }
  }

  /**
   * 自定义EntryProcessor，用于删除库存低于阈值的产品
   */
  public static class RemoveIfLowStockProcessor implements EntryProcessor<Long, Product, String>, Serializable {
    private static final long serialVersionUID = 1L;
    private final int minStock;

    public RemoveIfLowStockProcessor(int minStock) {
      this.minStock = minStock;
    }

    @Override
    public String process(MutableEntry<Long, Product> entry, Object... arguments) throws EntryProcessorException {
      if (entry.exists()) {
        Product product = entry.getValue();
        if (product.getStock() < minStock) {
          entry.remove();
          return "已删除 - 库存过低 (" + product.getStock() + " < " + minStock + ")";
        }
        return "保留 - 库存充足 (" + product.getStock() + " >= " + minStock + ")";
      }
      return "条目不存在";
    }
  }

  /**
   * 关闭缓存
   */
  private void closeCache() {
    if (cacheManager != null) {
      cacheManager.destroyCache(CACHE_NAME);
    }

    if (cachingProvider != null) {
      cachingProvider.close();
    }
  }
}