package org.example.hazelcast.demo.store;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.map.MapStore;
import org.example.hazelcast.demo.model.Product;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.CommandLineRunner;
import org.springframework.context.annotation.Lazy;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;

import javax.sql.DataSource;
import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import org.springframework.stereotype.Repository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Repository
public class ProductMapStore implements MapStore<Long, Product> , ApplicationRunner {

  private static final Logger logger = LoggerFactory.getLogger(ProductMapStore.class);
  private final JdbcTemplate jdbcTemplate;
  private static final int BATCH_SIZE = 50;

  public ProductMapStore(DataSource dataSource) {
    this.jdbcTemplate = new JdbcTemplate(dataSource);
    logger.info("ProductMapStore initialized with batch size: {}", BATCH_SIZE);
  }

  private final RowMapper<Product> productRowMapper = (rs, rowNum) -> new Product(
      rs.getLong("id"),
      rs.getString("name"),
      rs.getString("category"),
      rs.getBigDecimal("price"),
      rs.getInt("stock"));

  @Override
  public void store(Long key, Product product) {
    logger.debug("Storing product with ID: {}", key);
    jdbcTemplate.update(
        "REPLACE INTO product (id, name, category, price, stock) VALUES (?, ?, ?, ?, ?)",
        product.getId(), product.getName(), product.getCategory(), product.getPrice(), product.getStock());
  }

  @Override
  public void storeAll(Map<Long, Product> map) {
    if (map.isEmpty())
      return;

    logger.debug("Batch storing {} products", map.size());
    final List<Product> products = new ArrayList<>(map.values());

    jdbcTemplate.batchUpdate(
        "REPLACE INTO product (id, name, category, price, stock) VALUES (?, ?, ?, ?, ?)",
        new BatchPreparedStatementSetter() {
          @Override
          public void setValues(PreparedStatement ps, int i) throws SQLException {
            Product product = products.get(i);
            ps.setLong(1, product.getId());
            ps.setString(2, product.getName());
            ps.setString(3, product.getCategory());
            ps.setBigDecimal(4, product.getPrice());
            ps.setInt(5, product.getStock());
          }

          @Override
          public int getBatchSize() {
            return products.size();
          }
        });
  }

  @Override
  public void delete(Long key) {
    logger.debug("Deleting product with ID: {}", key);
    jdbcTemplate.update("DELETE FROM product WHERE id = ?", key);
  }

  @Override
  public void deleteAll(Collection<Long> keys) {
    if (keys.isEmpty())
      return;

    logger.debug("Batch deleting {} products", keys.size());

    // 用于IN子句的批量删除
    StringBuilder placeholders = new StringBuilder();
    for (int i = 0; i < keys.size(); i++) {
      placeholders.append(i == 0 ? "?" : ", ?");
    }

    String sql = "DELETE FROM product WHERE id IN (" + placeholders + ")";
    jdbcTemplate.update(sql, keys.toArray());
  }

  @Override
  public Product load(Long key) {
    logger.debug("Loading product with ID: {}", key);
    try {
      return jdbcTemplate.queryForObject(
          "SELECT * FROM product WHERE id = ?",
          productRowMapper,
          key);
    } catch (Exception e) {
      logger.debug("Product not found with ID: {}", key);
      return null;
    }
  }

  @Override
  public Map<Long, Product> loadAll(Collection<Long> keys) {
    if (keys.isEmpty())
      return Collections.emptyMap();

    logger.debug("Loading {} products", keys.size());
    Map<Long, Product> result = new HashMap<>();

    // 转换成List以支持分批操作
    List<Long> keysList = new ArrayList<>(keys);

    // 分批加载项
    int totalKeys = keysList.size();
    for (int i = 0; i < totalKeys; i += BATCH_SIZE) {
      int endIndex = Math.min(i + BATCH_SIZE, totalKeys);
      List<Long> batchKeys = keysList.subList(i, endIndex);
      loadBatch(batchKeys, result);
    }

    return result;
  }

  private void loadBatch(List<Long> batchKeys, Map<Long, Product> resultMap) {
    if (batchKeys.isEmpty())
      return;

    // 构建IN子句
    StringBuilder placeholders = new StringBuilder();
    for (int i = 0; i < batchKeys.size(); i++) {
      placeholders.append(i == 0 ? "?" : ", ?");
    }

    String sql = "SELECT * FROM product WHERE id IN (" + placeholders + ")";

    List<Product> products = jdbcTemplate.query(
        sql,
        batchKeys.toArray(),
        productRowMapper);

    // 保存到结果集
    for (Product product : products) {
      resultMap.put(product.getId(), product);
    }
  }

  @Override
  public Iterable<Long> loadAllKeys() {
    logger.info("Loading all product keys");
    return jdbcTemplate.query("SELECT id FROM product", (rs, rowNum) -> rs.getLong("id"));
  }

  // 根据类别获取产品键
  public List<Long> getKeysByCategory(String category) {
    logger.debug("Loading product keys by category: {}", category);
    return jdbcTemplate.query(
        "SELECT id FROM product WHERE category = ?",
        new Object[] { category },
        (rs, rowNum) -> rs.getLong("id"));
  }
  @Autowired
  @Lazy
  private HazelcastInstance hazelcastInstance;


  @Override
  public void run(ApplicationArguments args) throws Exception {
    IMap<Object, Object> map = hazelcastInstance.getMap("products");
    System.out.println(map);
  }
}