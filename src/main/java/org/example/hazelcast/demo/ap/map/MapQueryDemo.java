package org.example.hazelcast.demo.ap.map;

import com.hazelcast.config.Config;
import com.hazelcast.config.IndexConfig;
import com.hazelcast.config.IndexType;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.Predicates;
import org.example.hazelcast.demo.model.Product;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Hazelcast Map 查询和索引操作示例
 */
@Component
public class MapQueryDemo {

  private final HazelcastInstance hazelcastInstance;
  private final String MAP_NAME = "products";

  @Autowired
  public MapQueryDemo(HazelcastInstance hazelcastInstance) {
    this.hazelcastInstance = hazelcastInstance;
  }

  /**
   * 运行所有查询示例
   */
  public void runAllExamples() {
    System.out.println("=================== Hazelcast Map 查询和索引示例 ===================");
    prepareData();
    addIndexes();
    basicPredicateQueries();
    complexPredicateQueries();
    sqlQueries();
    pagingExample();
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
   * 添加索引
   */
  private void addIndexes() {
    System.out.println("\n--- 添加索引 ---");

    // 有两种方式添加索引

    // 1. 通过IMap直接添加
    IMap<Long, Product> productMap = hazelcastInstance.getMap(MAP_NAME);

    // 添加Category字段的有序索引
    productMap.addIndex(IndexType.SORTED, "category");
    System.out.println("已为Category字段添加排序索引");

    // 添加Price字段的有序索引
    productMap.addIndex(IndexType.SORTED, "price");
    System.out.println("已为Price字段添加排序索引");

    // 添加Stock字段的哈希索引
    productMap.addIndex(IndexType.HASH, "stock");
    System.out.println("已为Stock字段添加哈希索引");

    // 2. 通过配置添加索引(通常在启动时进行)
    // 这里仅作示例，实际上这种方式应该在Hazelcast实例初始化之前配置
    Config config = new Config();
    MapConfig mapConfig = config.getMapConfig(MAP_NAME);

    // 为Name字段添加索引
    IndexConfig nameIndexConfig = new IndexConfig(IndexType.HASH, "name");
    mapConfig.addIndexConfig(nameIndexConfig);

    System.out.println("注意：第二种方式应在Hazelcast实例启动前配置，此处仅作示例");
  }

  /**
   * 基本谓词查询示例
   */
  public void basicPredicateQueries() {
    System.out.println("\n--- 基本谓词查询示例 ---");

    IMap<Long, Product> productMap = hazelcastInstance.getMap(MAP_NAME);

    // 1. 等值查询(Equal)
    Predicate<Long, Product> equalPredicate = Predicates.equal("category", "配件");
    Collection<Product> accessories = productMap.values(equalPredicate);
    System.out.println("配件类别的产品数量: " + accessories.size());
    accessories.forEach(product -> System.out.println(" - " + product));

    // 2. 范围查询(Between)
    Predicate<Long, Product> pricePredicate = Predicates.between("price", new BigDecimal("1000"),
        new BigDecimal("5000"));
    Collection<Product> mediumPriceProducts = productMap.values(pricePredicate);
    System.out.println("\n价格在1000-5000之间的产品数量: " + mediumPriceProducts.size());
    mediumPriceProducts.forEach(product -> System.out.println(" - " + product));

    // 3. 大于/小于查询
    Predicate<Long, Product> stockPredicate = Predicates.greaterThan("stock", 200);
    Collection<Product> highStockProducts = productMap.values(stockPredicate);
    System.out.println("\n库存大于200的产品数量: " + highStockProducts.size());
    highStockProducts.forEach(product -> System.out.println(" - " + product));

    // 4. Like查询
    Predicate<Long, Product> likePredicate = Predicates.like("name", "%电脑%");
    Collection<Product> computerProducts = productMap.values(likePredicate);
    System.out.println("\n名称包含'电脑'的产品数量: " + computerProducts.size());
    computerProducts.forEach(product -> System.out.println(" - " + product));
  }

  /**
   * 复杂谓词查询示例
   */
  public void complexPredicateQueries() {
    System.out.println("\n--- 复杂谓词查询示例 ---");

    IMap<Long, Product> productMap = hazelcastInstance.getMap(MAP_NAME);

    // 1. AND 组合查询
    Predicate<Long, Product> andPredicate = Predicates.and(
        Predicates.equal("category", "电子产品"),
        Predicates.greaterThan("price", new BigDecimal("3000")));

    Collection<Product> expensiveElectronics = productMap.values(andPredicate);
    System.out.println("电子产品且价格大于3000的产品数量: " + expensiveElectronics.size());
    expensiveElectronics.forEach(product -> System.out.println(" - " + product));

    // 2. OR 组合查询
    Predicate<Long, Product> orPredicate = Predicates.or(
        Predicates.equal("category", "配件"),
        Predicates.lessThan("price", new BigDecimal("1000")));

    Collection<Product> accessoriesOrCheap = productMap.values(orPredicate);
    System.out.println("\n配件或价格低于1000的产品数量: " + accessoriesOrCheap.size());
    accessoriesOrCheap.forEach(product -> System.out.println(" - " + product));

    // 3. 复杂嵌套查询
    Predicate<Long, Product> complexPredicate = Predicates.and(
        Predicates.or(
            Predicates.equal("category", "电子产品"),
            Predicates.equal("category", "配件")),
        Predicates.greaterEqual("stock", 100),
        Predicates.lessThan("price", new BigDecimal("5000")));

    Collection<Product> complexResult = productMap.values(complexPredicate);
    System.out.println("\n(电子产品或配件)且库存>=100且价格<5000的产品数量: " + complexResult.size());
    complexResult.forEach(product -> System.out.println(" - " + product));
  }

  /**
   * SQL查询示例
   */
  public void sqlQueries() {
    System.out.println("\n--- SQL查询示例 ---");

    IMap<Long, Product> productMap = hazelcastInstance.getMap(MAP_NAME);

    // 1. 简单SQL查询
    Predicate<Long, Product> sqlPredicate1 = Predicates.sql("category = '电子产品'");
    Collection<Product> electronics = productMap.values(sqlPredicate1);
    System.out.println("SQL查询 - 电子产品数量: " + electronics.size());
    electronics.forEach(product -> System.out.println(" - " + product));

    // 2. 复杂SQL查询
    Predicate<Long, Product> sqlPredicate2 = Predicates.sql("price > 1000 AND stock < 100");
    Collection<Product> expensiveLowStock = productMap.values(sqlPredicate2);
    System.out.println("\nSQL查询 - 价格>1000且库存<100的产品数量: " + expensiveLowStock.size());
    expensiveLowStock.forEach(product -> System.out.println(" - " + product));

    // 3. LIKE操作符
    Predicate<Long, Product> sqlPredicate3 = Predicates.sql("name LIKE '%手机%'");
    Collection<Product> phoneProducts = productMap.values(sqlPredicate3);
    System.out.println("\nSQL查询 - 名称包含'手机'的产品数量: " + phoneProducts.size());
    phoneProducts.forEach(product -> System.out.println(" - " + product));
  }

  /**
   * 分页查询示例
   */
  public void pagingExample() {
    System.out.println("\n--- 分页查询示例 ---");

    IMap<Long, Product> productMap = hazelcastInstance.getMap(MAP_NAME);

    // 执行分页查询
    // 注意：Hazelcast没有内置的分页API，需要自行实现分页逻辑

    // 获取所有产品
    Collection<Product> allProducts = productMap.values();

    // 分页参数
    int pageSize = 3;
    int totalPages = (int) Math.ceil((double) allProducts.size() / pageSize);

    System.out.println("总产品数: " + allProducts.size() + ", 每页大小: " + pageSize + ", 总页数: " + totalPages);

    // 模拟分页
    for (int pageNum = 1; pageNum <= totalPages; pageNum++) {
      int startIdx = (pageNum - 1) * pageSize;
      int endIdx = Math.min(startIdx + pageSize, allProducts.size());

      System.out.println("\n第 " + pageNum + " 页:");
      allProducts.stream()
          .skip(startIdx)
          .limit(pageSize)
          .forEach(product -> System.out.println(" - " + product));
    }

    // 另一种分页方法：使用Predicate过滤特定范围的键
    // 对于大数据集，这种方法效率更高
    System.out.println("\n使用键范围进行分页查询 (第2页):");

    // 获取所有键并排序
    Object[] sortedKeys = productMap.keySet().stream().sorted().toArray();

    // 计算页面对应的键范围
    int page = 2;
    int startIndex = (page - 1) * pageSize;
    int endIndex = Math.min(startIndex + pageSize - 1, sortedKeys.length - 1);

    // 获取这个范围的键
    Object[] keysForPage = new Object[endIndex - startIndex + 1];
    System.arraycopy(sortedKeys, startIndex, keysForPage, 0, keysForPage.length);

    // 使用键获取值
    Set<Long> keysSet = new HashSet<>();
    for (Object key : keysForPage) {
      keysSet.add((Long) key);
    }
    Map<Long, Product> pageResults = productMap.getAll(keysSet);

    pageResults.forEach((k, v) -> System.out.println(" - 键: " + k + ", 值: " + v));
  }
}