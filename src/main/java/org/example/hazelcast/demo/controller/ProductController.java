package org.example.hazelcast.demo.controller;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.query.Predicates;
import org.example.hazelcast.demo.model.Product;
import org.example.hazelcast.demo.store.ProductMapStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * 产品控制器 - 提供产品数据的访问接口
 */
@RestController
@RequestMapping("/product")
public class ProductController {

  @Autowired
  private HazelcastInstance hazelcastInstance;

  @Autowired
  private ProductMapStore productMapStore;

  /**
   * 保存产品
   */
  @PostMapping
  public String save(@RequestBody Product product) {
    hazelcastInstance.getMap("products").put(product.getId(), product);
    return "Saved to Hazelcast (and DB via MapStore)";
  }

  /**
   * 获取单个产品
   */
  @GetMapping("/{id}")
  public Product get(@PathVariable Long id) {
    return (Product) hazelcastInstance.getMap("products").get(id);
  }

  /**
   * 删除产品
   */
  @DeleteMapping("/{id}")
  public String delete(@PathVariable Long id) {
    hazelcastInstance.getMap("products").delete(id);
    return "Deleted from Hazelcast (and DB via MapStore)";
  }

  /**
   * 获取所有产品
   */
  @GetMapping
  public Collection<Product> getAll() {
    IMap<Long, Product> map = hazelcastInstance.getMap("products");
    return map.values();
  }

  /**
   * 按类别获取产品
   */
  @GetMapping("/category/{category}")
  public List<Product> getByCategory(@PathVariable String category) {
    IMap<Long, Product> map = hazelcastInstance.getMap("products");

    // 使用Hazelcast内置的查询功能
    Collection<Product> products = map.values(Predicates.equal("category", category));
    return new ArrayList<>(products);
  }

  /**
   * 按价格范围获取产品
   */
  @GetMapping("/price-range")
  public List<Product> getByPriceRange(
      @RequestParam BigDecimal min,
      @RequestParam BigDecimal max) {
    IMap<Long, Product> map = hazelcastInstance.getMap("products");

    // 使用Hazelcast的范围查询
    Collection<Product> products = map.values(
        Predicates.between("price", min, max));
    return new ArrayList<>(products);
  }

  /**
   * 获取库存不足的产品
   */
  @GetMapping("/low-stock")
  public List<Product> getLowStockProducts(@RequestParam(defaultValue = "10") int threshold) {
    IMap<Long, Product> map = hazelcastInstance.getMap("products");

    Collection<Product> products = map.values(
        Predicates.lessThan("stock", threshold));
    return new ArrayList<>(products);
  }

  /**
   * 获取所有产品类别
   */
  @GetMapping("/categories")
  public List<String> getAllCategories() {
    IMap<Long, Product> map = hazelcastInstance.getMap("products");

    // 使用SQL查询获取唯一类别
    return map.values().stream()
        .map(Product::getCategory)
        .distinct()
        .toList();
  }

  /**
   * 添加示例产品数据
   */
  @PostMapping("/sample-data")
  public String addSampleData() {
    IMap<Long, Product> map = hazelcastInstance.getMap("products");

    // 添加示例产品
    map.put(1L, new Product(1L, "笔记本电脑", "电子产品", new BigDecimal("5999.00"), 100));
    map.put(2L, new Product(2L, "智能手机", "电子产品", new BigDecimal("3999.00"), 200));
    map.put(3L, new Product(3L, "无线耳机", "配件", new BigDecimal("999.00"), 500));
    map.put(4L, new Product(4L, "平板电脑", "电子产品", new BigDecimal("2999.00"), 150));
    map.put(5L, new Product(5L, "智能手表", "电子产品", new BigDecimal("1599.00"), 80));

    return "Added 5 sample products";
  }
}