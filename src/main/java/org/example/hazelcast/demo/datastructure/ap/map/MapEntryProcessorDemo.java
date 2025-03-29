package org.example.hazelcast.demo.datastructure.ap.map;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.IMap;
import org.example.hazelcast.demo.model.Product;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Hazelcast Map 入口处理器示例
 */
@Component
public class MapEntryProcessorDemo {

  private final HazelcastInstance hazelcastInstance;
  private final String MAP_NAME = "products";

  @Autowired
  public MapEntryProcessorDemo(HazelcastInstance hazelcastInstance) {
    this.hazelcastInstance = hazelcastInstance;
  }

  /**
   * 运行所有示例
   */
  public void runAllExamples() {
    System.out.println("=================== Hazelcast Map 入口处理器示例 ===================");
    prepareData();
    singleEntryProcessing();
    multipleEntryProcessing();
    conditionalEntryProcessing();
    complexEntryProcessing();
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
    productMap.put(3L, new Product(3L, "无线耳机", "配件", new BigDecimal("399.00"), 300));
    productMap.put(4L, new Product(4L, "平板电脑", "电子产品", new BigDecimal("2999.00"), 150));
    productMap.put(5L, new Product(5L, "机械键盘", "配件", new BigDecimal("199.00"), 200));

    System.out.println("已添加5个产品示例");
  }

  /**
   * 单一条目处理示例
   */
  public void singleEntryProcessing() {
    System.out.println("\n--- 单一条目处理示例 ---");

    IMap<Long, Product> productMap = hazelcastInstance.getMap(MAP_NAME);

    // 定义处理器：增加产品库存
    EntryProcessor<Long, Product, Integer> incrementStockProcessor = (entry) -> {
      Product product = entry.getValue();
      if (product != null) {
        // 增加库存
        int newStock = product.getStock() + 50;
        product.setStock(newStock);

        // 更新条目
        entry.setValue(product);

        // 返回新库存
        return newStock;
      }
      return null;
    };

    // 在单个条目上执行处理器
    Long targetKey = 1L;
    Integer newStock = productMap.executeOnKey(targetKey, incrementStockProcessor);

    System.out.println("ID为" + targetKey + "的产品库存已更新为: " + newStock);
    System.out.println("更新后的产品: " + productMap.get(targetKey));
  }

  /**
   * 多条目处理示例
   */
  public void multipleEntryProcessing() {
    System.out.println("\n--- 多条目处理示例 ---");

    IMap<Long, Product> productMap = hazelcastInstance.getMap(MAP_NAME);

    // 定义处理器：降低产品价格
    EntryProcessor<Long, Product, BigDecimal> discountProcessor = (entry) -> {
      Product product = entry.getValue();
      if (product != null) {
        // 计算折扣后价格 (打9折)
        BigDecimal originalPrice = product.getPrice();
        BigDecimal discountFactor = new BigDecimal("0.9");
        BigDecimal newPrice = originalPrice.multiply(discountFactor).setScale(2, BigDecimal.ROUND_HALF_UP);

        // 更新价格
        product.setPrice(newPrice);
        entry.setValue(product);

        // 返回新价格
        return newPrice;
      }
      return null;
    };

    // 在多个条目上执行处理器
    Set<Long> keysToDiscount = Set.of(2L, 3L, 4L);
    Map<Long, BigDecimal> results = productMap.executeOnKeys(keysToDiscount, discountProcessor);

    System.out.println("已对" + keysToDiscount.size() + "个产品应用折扣:");
    results.forEach((key, newPrice) -> {
      System.out.println(" - 产品ID: " + key + ", 新价格: " + newPrice);
    });
  }

  /**
   * 条件式条目处理示例
   */
  public void conditionalEntryProcessing() {
    System.out.println("\n--- 条件式条目处理示例 ---");

    IMap<Long, Product> productMap = hazelcastInstance.getMap(MAP_NAME);

    // 定义处理器：仅当库存低于阈值时增加库存
    EntryProcessor<Long, Product, String> conditionalStockProcessor = (entry) -> {
      Product product = entry.getValue();
      if (product != null) {
        if (product.getStock() < 200) {
          // 低库存，增加100
          int newStock = product.getStock() + 100;
          product.setStock(newStock);
          entry.setValue(product);
          return "已补充库存到: " + newStock;
        }
        return "库存充足，无需补充";
      }
      return "产品不存在";
    };

    // 在所有条目上执行条件处理器
    Map<Long, String> results = productMap.executeOnEntries(conditionalStockProcessor);

    System.out.println("条件库存处理结果:");
    results.forEach((key, result) -> {
      System.out.println(" - 产品ID: " + key + ", 结果: " + result);
    });
  }

  /**
   * 复杂条目处理示例
   */
  public void complexEntryProcessing() {
    System.out.println("\n--- 复杂条目处理示例 ---");

    IMap<Long, Product> productMap = hazelcastInstance.getMap(MAP_NAME);

    // 定义复杂处理器：基于类别和价格应用不同策略
    EntryProcessor<Long, Product, Map<String, Object>> complexProcessor = (entry) -> {
      Product product = entry.getValue();
      if (product == null) {
        return null;
      }

      Map<String, Object> result = new HashMap<>();
      result.put("oldPrice", product.getPrice());
      result.put("oldStock", product.getStock());

      String category = product.getCategory();
      BigDecimal price = product.getPrice();

      // 根据类别和价格应用不同策略
      if ("电子产品".equals(category)) {
        if (price.compareTo(new BigDecimal("3000")) > 0) {
          // 高价电子产品: 降价5%，库存不变
          BigDecimal newPrice = price.multiply(new BigDecimal("0.95")).setScale(2, BigDecimal.ROUND_HALF_UP);
          product.setPrice(newPrice);
          result.put("action", "高价电子产品降价");
          result.put("discount", "5%");
        } else {
          // 中低价电子产品: 价格不变，增加10%库存
          int newStock = (int) (product.getStock() * 1.1);
          product.setStock(newStock);
          result.put("action", "中低价电子产品增库存");
          result.put("stockIncrease", "10%");
        }
      } else {
        // 配件类别: 所有配件降价10%
        BigDecimal newPrice = price.multiply(new BigDecimal("0.9")).setScale(2, BigDecimal.ROUND_HALF_UP);
        product.setPrice(newPrice);
        result.put("action", "配件降价");
        result.put("discount", "10%");
      }

      // 更新条目
      entry.setValue(product);

      // 添加新值到结果
      result.put("newPrice", product.getPrice());
      result.put("newStock", product.getStock());

      return result;
    };

    // 执行复杂处理器
    Map<Long, Map<String, Object>> results = productMap.executeOnEntries(complexProcessor);

    System.out.println("复杂处理结果:");
    results.forEach((key, result) -> {
      Product product = productMap.get(key);
      System.out.println("\n产品ID: " + key + " (" + product.getName() + ")");
      System.out.println(" - 执行操作: " + result.get("action"));
      System.out.println(" - 旧价格: " + result.get("oldPrice") + ", 新价格: " + result.get("newPrice"));
      System.out.println(" - 旧库存: " + result.get("oldStock") + ", 新库存: " + result.get("newStock"));
      if (result.containsKey("discount")) {
        System.out.println(" - 折扣: " + result.get("discount"));
      }
      if (result.containsKey("stockIncrease")) {
        System.out.println(" - 库存增加: " + result.get("stockIncrease"));
      }
    });
  }
}