package org.example.hazelcast.demo.datastructure.ap.map;

import com.hazelcast.aggregation.Aggregators;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.projection.Projection;
import com.hazelcast.projection.Projections;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.Predicates;
import org.example.hazelcast.demo.model.Product;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Hazelcast Map 聚合操作示例
 */
@Component
public class MapAggregationDemo {

  private final HazelcastInstance hazelcastInstance;
  private final String MAP_NAME = "products";

  @Autowired
  public MapAggregationDemo(HazelcastInstance hazelcastInstance) {
    this.hazelcastInstance = hazelcastInstance;
  }

  /**
   * 运行所有聚合示例
   */
  public void runAllExamples() {
    System.out.println("=================== Hazelcast Map 聚合示例 ===================");
    prepareData();
    basicAggregations();
    conditionalAggregations();
    groupingAggregations();
    projectionExamples();
  }

  /**
   * 准备示例数据
   */
  private void prepareData() {
    System.out.println("\n--- 准备示例数据 ---");

    IMap<Long, Product> productMap = hazelcastInstance.getMap(MAP_NAME);
    productMap.clear();

    // 添加产品数据
    productMap.put(1L, new Product(1L, "高性能笔记本电脑", "电子产品", new BigDecimal("7999.00"), 50));
    productMap.put(2L, new Product(2L, "旗舰智能手机", "电子产品", new BigDecimal("4999.00"), 100));
    productMap.put(3L, new Product(3L, "无线耳机", "配件", new BigDecimal("399.00"), 300));
    productMap.put(4L, new Product(4L, "平板电脑", "电子产品", new BigDecimal("2999.00"), 150));
    productMap.put(5L, new Product(5L, "机械键盘", "配件", new BigDecimal("199.00"), 200));
    productMap.put(6L, new Product(6L, "游戏鼠标", "配件", new BigDecimal("99.00"), 400));
    productMap.put(7L, new Product(7L, "27英寸显示器", "电子产品", new BigDecimal("1499.00"), 80));
    productMap.put(8L, new Product(8L, "台式电脑", "电子产品", new BigDecimal("5999.00"), 30));
    productMap.put(9L, new Product(9L, "蓝牙音箱", "配件", new BigDecimal("299.00"), 250));
    productMap.put(10L, new Product(10L, "移动硬盘", "配件", new BigDecimal("599.00"), 180));

    System.out.println("已添加10个产品示例");
  }

  /**
   * 基本聚合操作示例
   */
  public void basicAggregations() {
    System.out.println("\n--- 基本聚合操作示例 ---");

    IMap<Long, Product> productMap = hazelcastInstance.getMap(MAP_NAME);

    // 1. 计数聚合
    long count = productMap.aggregate(Aggregators.count());
    System.out.println("产品总数: " + count);

    // 2. 最大值聚合
    BigDecimal maxPrice = productMap.aggregate(Aggregators.bigDecimalMax("price"));
    System.out.println("最高价格: " + maxPrice);

    // 3. 最小值聚合
    BigDecimal minPrice = productMap.aggregate(Aggregators.bigDecimalMin("price"));
    System.out.println("最低价格: " + minPrice);

    // 4. 求和聚合 (stock字段)
    long totalStock = productMap.aggregate(Aggregators.integerSum("stock"));
    System.out.println("总库存: " + totalStock);

    // 5. 平均值聚合
    double avgStock = productMap.aggregate(Aggregators.integerAvg("stock"));
    System.out.println("平均库存: " + avgStock);
  }

  /**
   * 条件聚合操作示例
   */
  public void conditionalAggregations() {
    System.out.println("\n--- 条件聚合操作示例 ---");

    IMap<Long, Product> productMap = hazelcastInstance.getMap(MAP_NAME);

    // 1. 使用谓词进行过滤，然后聚合
    Predicate<Long, Product> electronicsPredicate = Predicates.equal("category", "电子产品");

    // 计算电子产品的数量
    long electronicsCount = productMap.aggregate(Aggregators.count(), electronicsPredicate);
    System.out.println("电子产品数量: " + electronicsCount);

    // 计算电子产品的平均价格
    BigDecimal avgElectronicsPrice = productMap.aggregate(Aggregators.bigDecimalAvg("price"), electronicsPredicate);
    System.out.println("电子产品平均价格: " + avgElectronicsPrice);

    // 2. 使用复合谓词进行聚合
    Predicate<Long, Product> expensiveItemsPredicate = Predicates.greaterThan("price", new BigDecimal("3000"));
    Predicate<Long, Product> combinedPredicate = Predicates.and(electronicsPredicate, expensiveItemsPredicate);

    // 计算高价电子产品的总库存
    long expensiveElectronicsStock = productMap.aggregate(Aggregators.integerSum("stock"), combinedPredicate);
    System.out.println("价格大于3000的电子产品总库存: " + expensiveElectronicsStock);
  }

  /**
   * 分组聚合操作示例
   */
  public void groupingAggregations() {
    System.out.println("\n--- 分组聚合操作示例 ---");

    IMap<Long, Product> productMap = hazelcastInstance.getMap(MAP_NAME);

    // 创建分类到产品列表的映射
    Map<String, List<Product>> productsByCategory = new HashMap<>();

    // 手动分组
    for (Product product : productMap.values()) {
      productsByCategory.computeIfAbsent(product.getCategory(), k -> new ArrayList<>()).add(product);
    }

    // 1. 按类别分组计数
    Map<String, Long> countByCategory = new HashMap<>();
    productsByCategory.forEach((category, products) -> {
      countByCategory.put(category, (long) products.size());
    });

    System.out.println("按类别分组的产品数量:");
    countByCategory.forEach((category, count) -> {
      System.out.println("  " + category + ": " + count);
    });

    // 2. 按类别分组计算总库存
    Map<String, Long> stockByCategory = new HashMap<>();
    productsByCategory.forEach((category, products) -> {
      long totalStock = products.stream().mapToLong(Product::getStock).sum();
      stockByCategory.put(category, totalStock);
    });

    System.out.println("\n按类别分组的总库存:");
    stockByCategory.forEach((category, stock) -> {
      System.out.println("  " + category + ": " + stock);
    });

    // 3. 按类别分组计算平均价格
    Map<String, BigDecimal> avgPriceByCategory = new HashMap<>();
    productsByCategory.forEach((category, products) -> {
      BigDecimal totalPrice = products.stream()
          .map(Product::getPrice)
          .reduce(BigDecimal.ZERO, BigDecimal::add);
      BigDecimal avgPrice = totalPrice.divide(new BigDecimal(products.size()), 2, RoundingMode.HALF_UP);
      avgPriceByCategory.put(category, avgPrice);
    });

    System.out.println("\n按类别分组的平均价格:");
    avgPriceByCategory.forEach((category, avgPrice) -> {
      System.out.println("  " + category + ": " + avgPrice);
    });
  }

  /**
   * 投影操作示例
   */
  public void projectionExamples() {
    System.out.println("\n--- 投影操作示例 ---");

    IMap<Long, Product> productMap = hazelcastInstance.getMap(MAP_NAME);

    // 1. 单字段投影
    Collection<String> categoryProjection = productMap.project(Projections.singleAttribute("category"));
    System.out.println("所有类别: " + categoryProjection);

    // 2. 多字段投影 (使用自定义Projection)
    // 注: Hazelcast v5中可以使用更简洁的方式进行多字段投影
    @SuppressWarnings("unchecked")
    Projection<Map.Entry<Long, Product>, Object[]> multiProjection = entry -> {
      Product product = entry.getValue();
      return new Object[] { product.getName(), product.getPrice() };
    };

    Collection<Object[]> nameAndPriceProjection = productMap.project(multiProjection);
    System.out.println("\n名称和价格投影:");
    for (Object[] projection : nameAndPriceProjection) {
      System.out.println("  名称: " + projection[0] + ", 价格: " + projection[1]);
    }

    // 3. 谓词过滤后的投影
    Predicate<Long, Product> expensivePredicate = Predicates.greaterThan("price", new BigDecimal("1000"));
    Collection<String> expensiveNameProjection = productMap.project(
        Projections.singleAttribute("name"), expensivePredicate);

    System.out.println("\n价格大于1000的产品名称: " + expensiveNameProjection);
  }
}