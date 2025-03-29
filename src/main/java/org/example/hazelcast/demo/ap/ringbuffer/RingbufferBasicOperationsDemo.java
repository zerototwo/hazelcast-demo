package org.example.hazelcast.demo.ap.ringbuffer;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.ringbuffer.OverflowPolicy;
import com.hazelcast.ringbuffer.Ringbuffer;
import com.hazelcast.ringbuffer.ReadResultSet;
import com.hazelcast.core.IFunction;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

/**
 * Hazelcast Ringbuffer 基本操作示例
 * 
 * 官方定义：
 * Hazelcast Ringbuffer是一个循环数据结构，允许在尾部添加数据并从任何位置读取数据。
 * 它可以被视为一个循环数组，具有固定容量，一旦容量填满，新项目会覆盖最旧的项目。
 * 
 * 主要特性：
 * 1. 循环存储：固定大小的循环数组，新项目覆盖旧项目
 * 2. 顺序追加：始终从尾部添加新项目，确保顺序
 * 3. 灵活读取：可以从任意位置开始读取，支持历史和实时数据访问
 * 4. 批量操作：支持批量读取和写入，提高性能
 * 5. 背压处理：提供多种溢出处理策略
 * 6. 可靠性：支持数据持久化和备份
 * 7. 异步操作：支持异步读取和写入
 * 8. 高性能：针对大吞吐量和低延迟进行优化
 * 
 * 溢出策略：
 * - OVERWRITE：覆盖最旧的项目（默认）
 * - FAIL：如果容量已满则写入失败
 * - THROW_EXCEPTION：如果容量已满则抛出异常
 * - BLOCK：如果容量已满则阻塞直到有空间
 * 
 * 适用场景：
 * - 事件溯源和事件处理系统
 * - 实时数据流分析
 * - 日志和审计系统
 * - 有限历史记录的维护
 * - 环形缓冲区模式应用
 * - 消息订阅系统
 * - 时间窗口分析
 * 
 * Ringbuffer与其他数据结构的区别：
 * - 与Queue区别：Ringbuffer允许从任意位置读取，Queue只允许从头部读取
 * - 与List区别：Ringbuffer是固定容量的循环结构，List是可变大小的线性结构
 * - 与Topic区别：Ringbuffer存储历史消息并允许任意位置读取，Topic通常只发送给当前订阅者
 * 
 * 性能特点：
 * - 写入性能高：追加操作是恒定时间(O(1))
 * - 读取灵活：支持单个和批量读取
 * - 内存效率：固定容量设计避免无限增长
 * - 支持高吞吐量场景
 * 
 * 本示例演示了Hazelcast Ringbuffer的基本操作，包括添加、读取、异步操作、
 * 批量操作和溢出策略等核心功能。
 */
@Component
public class RingbufferBasicOperationsDemo {

  private final HazelcastInstance hazelcastInstance;
  private final String RINGBUFFER_NAME = "demo-ringbuffer";

  public RingbufferBasicOperationsDemo(HazelcastInstance hazelcastInstance) {
    this.hazelcastInstance = hazelcastInstance;
  }

  /**
   * 运行所有基本操作示例
   */
  public void runAllExamples() {
    System.out.println("=================== Hazelcast Ringbuffer 基本操作示例 ===================");

    basicOperationsExample();
    overflowPolicyExample();
    batchedAddExample();
    batchedReadExample();
    asyncMethodsExample();
    filteredReadExample();
  }

  /**
   * 基本操作示例
   */
  public void basicOperationsExample() {
    System.out.println("\n--- 基本 Ringbuffer 操作示例 ---");

    // 获取分布式 Ringbuffer
    Ringbuffer<String> ringbuffer = hazelcastInstance.getRingbuffer(RINGBUFFER_NAME);

    // 重置Ringbuffer，使用循环移除所有元素（Ringbuffer没有clear方法）
    try {
      while (ringbuffer.size() > 0) {
        ringbuffer.readOne(ringbuffer.headSequence());
      }
      System.out.println("Ringbuffer 已重置");
    } catch (Exception e) {
      System.out.println("Ringbuffer 重置: " + e.getMessage());
    }

    // 获取容量
    long capacity = ringbuffer.capacity();
    System.out.println("Ringbuffer 容量: " + capacity);

    // 添加元素
    long sequence1 = ringbuffer.add("Item-1");
    long sequence2 = ringbuffer.add("Item-2");
    long sequence3 = ringbuffer.add("Item-3");
    System.out.println("添加了3个元素，序列号为: " + sequence1 + ", " + sequence2 + ", " + sequence3);

    // 获取 Ringbuffer 大小
    long size = ringbuffer.size();
    System.out.println("Ringbuffer 大小: " + size);

    // 获取头部和尾部序列号
    long headSequence = ringbuffer.headSequence();
    long tailSequence = ringbuffer.tailSequence();
    System.out.println("头部序列号: " + headSequence + ", 尾部序列号: " + tailSequence);

    // 读取元素
    try {
      String item1 = ringbuffer.readOne(headSequence);
      String item2 = ringbuffer.readOne(headSequence + 1);
      String item3 = ringbuffer.readOne(headSequence + 2);
      System.out.println("读取元素: " + item1 + ", " + item2 + ", " + item3);
    } catch (Exception e) {
      System.err.println("读取元素时出错: " + e.getMessage());
    }

    // 获取剩余容量
    long remainingCapacity = ringbuffer.remainingCapacity();
    System.out.println("剩余容量: " + remainingCapacity);
  }

  /**
   * 溢出策略示例
   */
  public void overflowPolicyExample() {
    System.out.println("\n--- 溢出策略示例 ---");

    // 获取分布式 Ringbuffer 并设置较小的容量以演示溢出
    Ringbuffer<String> ringbuffer = hazelcastInstance.getRingbuffer(RINGBUFFER_NAME);

    // 重置Ringbuffer
    resetRingbuffer(ringbuffer);

    System.out.println("添加5个元素到 Ringbuffer");
    for (int i = 1; i <= 5; i++) {
      ringbuffer.add("OverflowItem-" + i);
    }

    System.out.println("当前 Ringbuffer 大小: " + ringbuffer.size());
    System.out.println("头部序列号: " + ringbuffer.headSequence() + ", 尾部序列号: " + ringbuffer.tailSequence());

    // 演示不同的溢出策略
    try {
      System.out.println("\n1. OVERWRITE 策略 - 当 Ringbuffer 已满时覆盖最旧的项");
      CompletableFuture<Long> future1 = ringbuffer.addAsync("OVERWRITE策略项", OverflowPolicy.OVERWRITE)
          .toCompletableFuture();
      long sequence = future1.join();
      System.out.println("项目已添加，序列号: " + sequence);
      System.out.println("头部序列号现在是: " + ringbuffer.headSequence());

      System.out.println("\n2. FAIL 策略 - 当 Ringbuffer 已满时失败");
      // 先填满 Ringbuffer
      resetRingbuffer(ringbuffer);
      long capacity = ringbuffer.capacity();
      for (int i = 0; i < capacity; i++) {
        ringbuffer.add("Item-" + i);
      }
      try {
        CompletableFuture<Long> future2 = ringbuffer.addAsync("FAIL策略项", OverflowPolicy.FAIL).toCompletableFuture();
        future2.join();
      } catch (Exception e) {
        System.out.println("预期的失败: " + e.getCause().getMessage());
      }
    } catch (Exception e) {
      System.err.println("溢出策略示例出错: " + e.getMessage());
    }
  }

  /**
   * 批量添加示例
   */
  public void batchedAddExample() {
    System.out.println("\n--- 批量添加示例 ---");

    // 获取分布式 Ringbuffer
    Ringbuffer<String> ringbuffer = hazelcastInstance.getRingbuffer(RINGBUFFER_NAME);
    resetRingbuffer(ringbuffer);

    List<String> items = Arrays.asList("Batch-1", "Batch-2", "Batch-3", "Batch-4", "Batch-5");
    try {
      System.out.println("批量添加 " + items.size() + " 个元素");
      CompletableFuture<Long> future = ringbuffer.addAllAsync(items, OverflowPolicy.OVERWRITE).toCompletableFuture();
      long lastSequence = future.join();
      System.out.println("批量添加完成，最后的序列号: " + lastSequence);
      System.out.println("Ringbuffer 大小: " + ringbuffer.size());
      System.out.println("头部序列号: " + ringbuffer.headSequence() + ", 尾部序列号: " + ringbuffer.tailSequence());
    } catch (Exception e) {
      System.err.println("批量添加出错: " + e.getMessage());
    }
  }

  /**
   * 批量读取示例
   */
  public void batchedReadExample() {
    System.out.println("\n--- 批量读取示例 ---");

    // 获取分布式 Ringbuffer
    Ringbuffer<String> ringbuffer = hazelcastInstance.getRingbuffer(RINGBUFFER_NAME);

    // 确保有数据可读
    if (ringbuffer.size() == 0) {
      for (int i = 1; i <= 10; i++) {
        ringbuffer.add("BatchReadItem-" + i);
      }
    }

    try {
      long startSequence = ringbuffer.headSequence();
      System.out.println("从序列号 " + startSequence + " 开始批量读取");

      // 最小读取1个，最大读取5个，无过滤器
      CompletableFuture<ReadResultSet<String>> future = ringbuffer.readManyAsync(startSequence, 1, 5, null)
          .toCompletableFuture();

      ReadResultSet<String> resultSet = future.join();
      System.out.println("批量读取完成，实际读取数量: " + resultSet.readCount());

      for (String item : resultSet) {
        System.out.println(" - 读取项: " + item);
      }

      System.out.println("下一个序列号应该是: " + (startSequence + resultSet.readCount()));
    } catch (Exception e) {
      System.err.println("批量读取出错: " + e.getMessage());
    }
  }

  /**
   * 异步方法示例
   */
  public void asyncMethodsExample() {
    System.out.println("\n--- 异步方法示例 ---");

    // 获取分布式 Ringbuffer
    Ringbuffer<String> ringbuffer = hazelcastInstance.getRingbuffer(RINGBUFFER_NAME);
    resetRingbuffer(ringbuffer);

    try {
      System.out.println("使用异步方法添加元素");

      // 异步添加并等待完成
      CompletionStage<Long> addStage = ringbuffer.addAsync("AsyncItem-1", OverflowPolicy.OVERWRITE);
      addStage.thenAccept(sequence -> System.out.println("元素已添加，序列号: " + sequence));

      // 让异步操作有时间完成
      TimeUnit.MILLISECONDS.sleep(100);

      // 链式异步操作
      System.out.println("\n链式异步操作");
      ringbuffer.addAsync("AsyncItem-2", OverflowPolicy.OVERWRITE)
          .thenComposeAsync(seq -> ringbuffer.addAsync("AsyncItem-3", OverflowPolicy.OVERWRITE))
          .thenComposeAsync(seq -> ringbuffer.addAsync("AsyncItem-4", OverflowPolicy.OVERWRITE))
          .thenAccept(finalSeq -> System.out.println("链式操作完成，最终序列号: " + finalSeq));

      // 让异步操作有时间完成
      TimeUnit.MILLISECONDS.sleep(100);

      System.out.println("\nRingbuffer 当前大小: " + ringbuffer.size());
    } catch (Exception e) {
      System.err.println("异步方法示例出错: " + e.getMessage());
    }
  }

  /**
   * 过滤读取示例
   */
  public void filteredReadExample() {
    System.out.println("\n--- 过滤读取示例 ---");

    // 获取分布式 Ringbuffer
    Ringbuffer<String> ringbuffer = hazelcastInstance.getRingbuffer(RINGBUFFER_NAME);
    resetRingbuffer(ringbuffer);

    // 添加各种前缀的元素
    ringbuffer.add("apple-1");
    ringbuffer.add("banana-1");
    ringbuffer.add("apple-2");
    ringbuffer.add("cherry-1");
    ringbuffer.add("apple-3");
    ringbuffer.add("banana-2");
    System.out.println("添加了6个元素到 Ringbuffer");

    try {
      long startSequence = ringbuffer.headSequence();

      // 因为Hazelcast的过滤器接口不容易使用lambda，这里我们创建一个实现该接口的内部类
      class AppleFilter implements IFunction<String, Boolean> {
        @Override
        public Boolean apply(String item) {
          return item.startsWith("apple");
        }
      }

      System.out.println("使用过滤器读取（只读取 'apple' 开头的项目）");
      CompletableFuture<ReadResultSet<String>> future = ringbuffer
          .readManyAsync(startSequence, 1, 10, new AppleFilter()).toCompletableFuture();

      ReadResultSet<String> resultSet = future.join();
      System.out.println("过滤读取完成，读取数量: " + resultSet.readCount() + ", 过滤后项目数: " + resultSet.size());

      for (String item : resultSet) {
        System.out.println(" - 过滤后的项: " + item);
      }
    } catch (Exception e) {
      System.err.println("过滤读取示例出错: " + e.getMessage());
    }
  }

  /**
   * 辅助方法：重置Ringbuffer（由于没有clear方法）
   */
  private void resetRingbuffer(Ringbuffer<String> ringbuffer) {
    try {
      // 移除所有元素或重新创建
      while (ringbuffer.size() > 0) {
        ringbuffer.readOne(ringbuffer.headSequence());
      }
    } catch (Exception e) {
      // 忽略异常，可能是空的或其他原因
    }
  }
}