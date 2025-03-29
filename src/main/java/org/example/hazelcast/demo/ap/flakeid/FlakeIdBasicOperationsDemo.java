package org.example.hazelcast.demo.ap.flakeid;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.flakeidgen.FlakeIdGenerator;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Hazelcast Flake ID Generator 基本操作示例
 * 
 * 此类展示了Hazelcast分布式Flake ID生成器的基本用法和特性。
 * Flake ID Generator生成全局唯一的64位ID，由三部分组成：
 * 1. 41位时间戳 - 提供~70年的ID生成期限
 * 2. 16位节点ID - 支持多达65536个不同的节点标识
 * 3. 6位序列号 - 每毫秒允许生成64个ID
 * 
 * Flake ID Generator的主要特性包括：
 * - 全局唯一性：保证在分布式系统中生成的ID不重复
 * - 近似排序：ID按时间近似排序，便于索引和分片
 * - 高性能：通过预取批次机制提供极高的吞吐量
 * - 容错：即使节点失败，也能继续生成唯一ID
 * - 分布式友好：无需中央协调器，适合分布式系统
 * 
 * 本类通过一系列示例展示这些特性，包括：
 * - 基本ID生成
 * - 唯一性验证
 * - 排序特性演示
 * - 性能测试
 * - 多线程批量生成
 */
@Component
public class FlakeIdBasicOperationsDemo {

  private final HazelcastInstance hazelcastInstance;
  private final String GENERATOR_NAME = "demo-flake-id-generator";

  /**
   * 构造函数，注入Hazelcast实例
   * 
   * @param hazelcastInstance 用于访问Hazelcast分布式服务的实例
   */
  public FlakeIdBasicOperationsDemo(HazelcastInstance hazelcastInstance) {
    this.hazelcastInstance = hazelcastInstance;
  }

  /**
   * 运行所有基本操作示例
   * 
   * 按顺序展示Flake ID Generator的各种基本特性和操作：
   * 1. 创建和获取ID
   * 2. 验证ID唯一性
   * 3. 展示ID的排序特性
   * 4. 测试ID生成性能
   * 5. 多线程批量生成ID
   */
  public void runAllExamples() {
    System.out.println("=================== Hazelcast Flake ID Generator 基本操作示例 ===================");

    basicIdGenerationExample();
    uniquenessExample();
    orderingExample();
    performanceExample();
    multithreadedGenerationExample();
  }

  /**
   * 基本ID生成示例
   * 
   * 展示如何创建Flake ID Generator实例并生成ID。
   * 演示生成的ID格式和特性，对比连续生成的ID结构。
   */
  public void basicIdGenerationExample() {
    System.out.println("\n--- 基本ID生成示例 ---");

    // 获取或创建名为"basic-id-generator"的Flake ID生成器
    // 相同名称的生成器在集群中是同一个实例
    FlakeIdGenerator idGenerator = hazelcastInstance.getFlakeIdGenerator("basic-id-generator");

    // 生成几个ID并展示
    System.out.println("生成5个连续的ID:");
    for (int i = 0; i < 5; i++) {
      // newId()调用生成并返回一个新的、唯一的64位ID
      long id = idGenerator.newId();
      System.out.println("ID " + (i + 1) + ": " + id);
    }

    // 解释Flake ID的结构
    System.out.println("\nFlake ID结构说明:");
    System.out.println("- 每个ID是一个64位(最高位为符号位)的长整型数字");
    System.out.println("- ID由三部分组成: 时间戳(41位) + 节点ID(16位) + 序列号(6位)");
    System.out.println("- 这种结构保证了ID的全局唯一性和近似时间排序特性");
  }

  /**
   * 唯一性示例
   * 
   * 验证生成的多个ID之间没有重复，证明Flake ID的唯一性特性。
   * 此示例生成大量ID并检查是否存在重复。
   */
  public void uniquenessExample() {
    System.out.println("\n--- 唯一性示例 ---");

    // 获取Flake ID生成器实例
    FlakeIdGenerator idGenerator = hazelcastInstance.getFlakeIdGenerator("uniqueness-id-generator");

    // 生成ID的数量
    int count = 100_000;
    System.out.println("生成 " + count + " 个ID并检查唯一性...");

    // 使用Set来检测重复
    Set<Long> idSet = new HashSet<>(count);
    int duplicates = 0;

    // 生成指定数量的ID并检查重复
    for (int i = 0; i < count; i++) {
      long id = idGenerator.newId();
      if (!idSet.add(id)) {
        // 如果添加失败，表示ID已存在，即发现重复
        duplicates++;
        System.out.println("发现重复ID: " + id);
      }
    }

    // 报告结果
    if (duplicates == 0) {
      System.out.println("验证完成: 所有 " + count + " 个ID都是唯一的");
    } else {
      System.out.println("警告: 发现 " + duplicates + " 个重复ID");
    }
  }

  /**
   * 排序特性示例
   * 
   * 展示Flake ID的近似时间排序特性。
   * 生成的ID大体上按时间顺序增长，但由于多线程和预取机制，
   * 不保证严格的单调递增。
   */
  public void orderingExample() {
    System.out.println("\n--- 排序特性示例 ---");

    // 获取Flake ID生成器实例
    FlakeIdGenerator idGenerator = hazelcastInstance.getFlakeIdGenerator("ordering-id-generator");

    System.out.println("演示Flake ID的时间排序特性:");

    // 测试前后两批ID的大小关系
    System.out.println("生成第一批ID...");
    List<Long> firstBatch = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      firstBatch.add(idGenerator.newId());
    }

    // 添加延迟使时间戳部分明显不同
    try {
      System.out.println("等待1秒...");
      Thread.sleep(1000);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }

    System.out.println("生成第二批ID...");
    List<Long> secondBatch = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      secondBatch.add(idGenerator.newId());
    }

    // 展示两批ID
    System.out.println("\n第一批ID:");
    for (Long id : firstBatch) {
      System.out.println("  " + id);
    }

    System.out.println("\n第二批ID:");
    for (Long id : secondBatch) {
      System.out.println("  " + id);
    }

    // 验证排序特性
    System.out.println("\n验证排序特性:");
    Long minFirstBatch = firstBatch.stream().min(Long::compare).orElse(0L);
    Long maxFirstBatch = firstBatch.stream().max(Long::compare).orElse(0L);
    Long minSecondBatch = secondBatch.stream().min(Long::compare).orElse(0L);
    Long maxSecondBatch = secondBatch.stream().max(Long::compare).orElse(0L);

    System.out.println("第一批ID范围: " + minFirstBatch + " 到 " + maxFirstBatch);
    System.out.println("第二批ID范围: " + minSecondBatch + " 到 " + maxSecondBatch);

    if (minSecondBatch > maxFirstBatch) {
      System.out.println("结论: 第二批所有ID都大于第一批所有ID，验证了时间排序特性");
    } else {
      System.out.println("结论: 两批ID之间存在一些重叠，这可能是由于预取机制或集群同步延迟导致的");
    }
  }

  /**
   * 性能测试示例
   * 
   * 测量Flake ID Generator的性能特性，包括吞吐量和延迟。
   * 生成大量ID并测量所需时间，计算每秒生成ID的数量。
   */
  public void performanceExample() {
    System.out.println("\n--- 性能测试示例 ---");

    // 获取Flake ID生成器实例
    FlakeIdGenerator idGenerator = hazelcastInstance.getFlakeIdGenerator("performance-id-generator");

    // 设置测试参数
    int numIds = 1_000_000; // 生成的ID总数
    System.out.println("生成 " + numIds + " 个ID以测试性能...");

    // 记录开始时间
    long startTime = System.currentTimeMillis();

    // 生成指定数量的ID
    for (int i = 0; i < numIds; i++) {
      idGenerator.newId();
    }

    // 计算总耗时和性能指标
    long endTime = System.currentTimeMillis();
    long duration = endTime - startTime;
    double idsPerSecond = numIds * 1000.0 / duration;

    // 报告结果
    System.out.println("性能测试结果:");
    System.out.println("- 总耗时: " + duration + " 毫秒");
    System.out.println("- 平均每秒生成ID数: " + String.format("%.2f", idsPerSecond) + " 个/秒");
    System.out.println("- 平均每个ID生成耗时: " + String.format("%.3f", duration * 1.0 / numIds) + " 毫秒");

    // 解释性能特点
    System.out.println("\nFlake ID Generator性能特点:");
    System.out.println("- 高性能得益于预取机制，一次网络请求批量获取多个ID");
    System.out.println("- 本地缓存ID用尽后才会发起新的网络请求");
    System.out.println("- 性能受网络延迟、集群规模和预取配置影响");
  }

  /**
   * 多线程批量生成示例
   * 
   * 演示在多线程环境下使用Flake ID Generator的情况。
   * 创建多个线程并发生成ID，验证并发安全性和唯一性保证。
   */
  public void multithreadedGenerationExample() {
    System.out.println("\n--- 多线程批量生成示例 ---");

    // 获取Flake ID生成器实例
    FlakeIdGenerator idGenerator = hazelcastInstance.getFlakeIdGenerator("multithreaded-id-generator");

    // 设置测试参数
    int numThreads = 10; // 线程数
    int idsPerThread = 100_000; // 每个线程生成的ID数

    System.out.println("使用 " + numThreads + " 个线程，每个线程生成 " + idsPerThread + " 个ID...");

    // 用于检测重复的集合 - 使用线程安全的方式访问
    final Set<Long> allIds = new HashSet<>(numThreads * idsPerThread);
    final AtomicLong duplicateCount = new AtomicLong(0);

    // 创建并发任务
    List<CompletableFuture<Void>> futures = new ArrayList<>();

    // 记录开始时间
    long startTime = System.currentTimeMillis();

    // 启动多线程生成任务
    for (int t = 0; t < numThreads; t++) {
      final int threadId = t;
      CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
        for (int i = 0; i < idsPerThread; i++) {
          long id = idGenerator.newId();
          // 同步块中检查并记录ID，避免并发修改异常
          synchronized (allIds) {
            if (!allIds.add(id)) {
              duplicateCount.incrementAndGet();
              System.out.println("线程 " + threadId + " 发现重复ID: " + id);
            }
          }
        }
      });
      futures.add(future);
    }

    // 等待所有线程完成
    CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

    // 计算性能指标
    long endTime = System.currentTimeMillis();
    long duration = endTime - startTime;
    int totalIds = numThreads * idsPerThread;
    double idsPerSecond = totalIds * 1000.0 / duration;

    // 报告结果
    System.out.println("\n多线程生成结果:");
    System.out.println("- 总共生成ID数: " + totalIds);
    System.out.println("- 总耗时: " + duration + " 毫秒");
    System.out.println("- 平均每秒生成ID数: " + String.format("%.2f", idsPerSecond) + " 个/秒");

    // 报告唯一性检查结果
    if (duplicateCount.get() == 0) {
      System.out.println("- 唯一性验证: 通过，没有发现重复ID");
    } else {
      System.out.println("- 唯一性验证: 失败，发现 " + duplicateCount.get() + " 个重复ID");
    }

    // 比较单线程与多线程性能
    System.out.println("\n多线程环境下的Flake ID Generator特点:");
    System.out.println("- 线程安全：多线程环境下保证ID唯一性");
    System.out.println("- 高并发：支持多线程并发生成，性能随线程数提升");
    System.out.println("- 竞争处理：多线程共享预取缓存，减少竞争");
  }
}