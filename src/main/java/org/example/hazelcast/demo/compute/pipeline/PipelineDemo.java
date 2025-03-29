package org.example.hazelcast.demo.compute.pipeline;

import com.hazelcast.core.HazelcastInstance;
import org.springframework.stereotype.Component;

/**
 * 演示Hazelcast Pipeline API的使用方法。
 * 
 * Pipeline是Hazelcast的高级数据处理API，用于创建数据流或批处理管道，
 * 允许您在集群上运行复杂的分布式计算。
 * 
 * 主要特点：
 * - 声明式API：使用功能性编程风格定义处理步骤
 * - 分布式执行：自动在集群中分发和并行执行
 * - 容错：内置的容错机制确保可靠的处理
 * - 支持流处理和批处理：适用于各种处理场景
 */
@Component
public class PipelineDemo {

  private final HazelcastInstance hazelcastInstance;

  public PipelineDemo(HazelcastInstance hazelcastInstance) {
    this.hazelcastInstance = hazelcastInstance;
  }

  /**
   * 运行所有Pipeline示例
   */
  public void runAllExamples() {
    System.out.println("===== Pipeline API 是Hazelcast企业版的高级功能 =====");
    System.out.println("Pipeline API支持以下功能：");
    System.out.println("- 批处理和流处理的统一API");
    System.out.println("- 高性能的分布式数据处理");
    System.out.println("- 内置弹性和容错");
    System.out.println("- 丰富的变换、聚合和连接操作");
    System.out.println("- 与Hazelcast其他数据结构的无缝集成");
    System.out.println();

    showBasicPipelineExample();
    System.out.println();

    showFilterMapPipelineExample();
    System.out.println();

    showAggregationPipelineExample();
    System.out.println();

    showJoinPipelineExample();
    System.out.println("===============================================");
  }

  /**
   * 基本Pipeline示例
   * 展示Pipeline如何创建简单数据转换
   */
  public void basicPipelineExample() {
    showBasicPipelineExample();
  }

  private void showBasicPipelineExample() {
    System.out.println("--- Basic Pipeline Example ---");
    System.out.println("基本Pipeline示例代码（示意）：");
    System.out.println("Pipeline pipeline = Pipeline.create();");
    System.out.println("pipeline");
    System.out.println("  .readFrom(Sources.list(\"source-list\"))");
    System.out.println("  .map(String::toUpperCase)");
    System.out.println("  .writeTo(Sinks.map(\"result-map\"));");
    System.out.println();
    System.out.println("输入数据: [\"one\", \"two\", \"three\"]");
    System.out.println("输出数据: [\"ONE\", \"TWO\", \"THREE\"]");
  }

  /**
   * 过滤和映射Pipeline示例
   * 展示如何在Pipeline中使用过滤和映射操作
   */
  public void filterMapPipelineExample() {
    showFilterMapPipelineExample();
  }

  private void showFilterMapPipelineExample() {
    System.out.println("--- Filter and Map Pipeline Example ---");
    System.out.println("过滤和映射示例代码（示意）：");
    System.out.println("pipeline");
    System.out.println("  .readFrom(Sources.list(\"products\"))");
    System.out.println("  .filter(product -> product.getPrice() > 10.0)");
    System.out.println("  .map(product -> String.format(\"%s: $%.2f\", product.getName(), product.getPrice()))");
    System.out.println("  .writeTo(Sinks.list(\"expensive-products\"));");
    System.out.println();
    System.out.println("输入产品: [");
    System.out.println("  {\"name\": \"苹果\", \"price\": 5.5, \"category\": \"水果\"},");
    System.out.println("  {\"name\": \"蛋糕\", \"price\": 12.0, \"category\": \"烘焙\"},");
    System.out.println("  {\"name\": \"咖啡\", \"price\": 18.0, \"category\": \"饮料\"}");
    System.out.println("]");
    System.out.println("输出结果: [");
    System.out.println("  \"蛋糕: $12.00\",");
    System.out.println("  \"咖啡: $18.00\"");
    System.out.println("]");
  }

  /**
   * 聚合Pipeline示例
   * 展示如何在Pipeline中使用聚合操作
   */
  public void aggregationPipelineExample() {
    showAggregationPipelineExample();
  }

  private void showAggregationPipelineExample() {
    System.out.println("--- Aggregation Pipeline Example ---");
    System.out.println("聚合操作示例代码（示意）：");
    System.out.println("pipeline");
    System.out.println("  .readFrom(Sources.list(\"orders\"))");
    System.out.println("  .groupingKey(order -> order.getProductName())");
    System.out.println("  .aggregate(AggregateOperations.summingDouble(");
    System.out.println("      order -> order.getQuantity() * order.getPrice()))");
    System.out.println("  .writeTo(Sinks.map(\"product-totals\"));");
    System.out.println();
    System.out.println("输入订单: [");
    System.out.println("  {\"id\": 1001, \"product\": \"苹果\", \"quantity\": 5, \"price\": 5.5},");
    System.out.println("  {\"id\": 1002, \"product\": \"香蕉\", \"quantity\": 3, \"price\": 3.2},");
    System.out.println("  {\"id\": 1003, \"product\": \"苹果\", \"quantity\": 2, \"price\": 5.5}");
    System.out.println("]");
    System.out.println("输出结果: {");
    System.out.println("  \"苹果\": 38.5,  // (5*5.5) + (2*5.5)");
    System.out.println("  \"香蕉\": 9.6     // (3*3.2)");
    System.out.println("}");
  }

  /**
   * 连接(Join)Pipeline示例
   * 展示如何在Pipeline中连接两个数据流
   */
  public void joinPipelineExample() {
    showJoinPipelineExample();
  }

  private void showJoinPipelineExample() {
    System.out.println("--- Join Pipeline Example ---");
    System.out.println("连接操作示例代码（示意）：");
    System.out.println("BatchStage<Student> students = pipeline.readFrom(Sources.list(\"students\"));");
    System.out.println("BatchStage<Course> courses = pipeline.readFrom(Sources.list(\"courses\"));");
    System.out.println();
    System.out.println("students.hashJoin(");
    System.out.println("  courses,");
    System.out.println("  JoinClause.joinMapEntries(");
    System.out.println("    Student::getId,");
    System.out.println("    Course::getStudentId");
    System.out.println("  ),");
    System.out.println("  (student, course) -> String.format(");
    System.out.println("    \"学生: %s, 课程: %s\",");
    System.out.println("    student.getName(), course.getName()");
    System.out.println("  )");
    System.out.println(").writeTo(Sinks.list(\"student-courses\"));");
    System.out.println();
    System.out.println("学生数据: [");
    System.out.println("  {\"id\": 1, \"name\": \"张三\", \"age\": 20},");
    System.out.println("  {\"id\": 2, \"name\": \"李四\", \"age\": 22}");
    System.out.println("]");
    System.out.println("课程数据: [");
    System.out.println("  {\"id\": 101, \"name\": \"数学\", \"studentId\": 1},");
    System.out.println("  {\"id\": 102, \"name\": \"物理\", \"studentId\": 1},");
    System.out.println("  {\"id\": 103, \"name\": \"化学\", \"studentId\": 2}");
    System.out.println("]");
    System.out.println("连接结果: [");
    System.out.println("  \"学生: 张三, 课程: 数学\",");
    System.out.println("  \"学生: 张三, 课程: 物理\",");
    System.out.println("  \"学生: 李四, 课程: 化学\"");
    System.out.println("]");
  }
}