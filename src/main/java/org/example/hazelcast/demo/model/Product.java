package org.example.hazelcast.demo.model;

import java.io.Serializable;
import java.math.BigDecimal;

/**
 * 产品模型类，用于Hazelcast Map示例
 */
public class Product implements Serializable {
  private static final long serialVersionUID = 1L;

  private Long id;
  private String name;
  private String category;
  private BigDecimal price;
  private Integer stock;

  public Product() {
  }

  public Product(Long id, String name, String category, BigDecimal price, Integer stock) {
    this.id = id;
    this.name = name;
    this.category = category;
    this.price = price;
    this.stock = stock;
  }

  // Getters and Setters
  public Long getId() {
    return id;
  }

  public void setId(Long id) {
    this.id = id;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getCategory() {
    return category;
  }

  public void setCategory(String category) {
    this.category = category;
  }

  public BigDecimal getPrice() {
    return price;
  }

  public void setPrice(BigDecimal price) {
    this.price = price;
  }

  public Integer getStock() {
    return stock;
  }

  public void setStock(Integer stock) {
    this.stock = stock;
  }

  @Override
  public String toString() {
    return "Product{" +
        "id=" + id +
        ", name='" + name + '\'' +
        ", category='" + category + '\'' +
        ", price=" + price +
        ", stock=" + stock +
        '}';
  }
}