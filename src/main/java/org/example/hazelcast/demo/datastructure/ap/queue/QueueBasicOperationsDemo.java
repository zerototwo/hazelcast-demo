package org.example.hazelcast.demo.datastructure.ap.queue;

import com.hazelcast.collection.IQueue;
import com.hazelcast.collection.ItemListener;
import com.hazelcast.collection.ItemEvent;
import com.hazelcast.core.HazelcastInstance;
import org.example.hazelcast.demo.model.Product;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Hazelcast Queue 基本操作示例
 * 
 * 官方定义：
 * Hazelcast分布式队列(IQueue)是java.util.concurrent.BlockingQueue接口的分布式实现。
 * 队列中的项目可以分区到多个集群成员，同时保持FIFO(先进先出)的语义。
 * 
 * 主要特性：
 * 1. FIFO保证：元素按照放入顺序被严格取出
 * 2. 分布式实现：数据在集群成员间分区存储
 * 3. 阻塞操作：支持在队列空/满时进行阻塞操作
 * 4. 非阻塞操作：提供立即返回的添加/移除操作
 * 5. 超时机制：支持在指定时间内等待队列操作
 * 6. 事件通知：支持队列项目添加/移除时的监听
 * 7. 队列排空：支持批量取出队列中的多个项目
 * 8. 优先级队列：支持基于优先级的队列排序
 * 9. 有界队列：可以设置队列的最大容量
 * 
 * 适用场景：
 * - 任务分发和工作队列
 * - 微服务间的异步通信
 * - 缓冲区处理
 * - 生产者-消费者模式的实现
 * - 任务调度系统
 * - 负载均衡和请求节流
 * 
 * 配置选项：
 * - 备份数：控制队列数据的备份副本数
 * - 最大大小：限制队列可容纳的最大项目数
 * - 空队列TTL：设置空队列在自动销毁前的存在时间
 * - 分裂保护：防止网络分区情况下的数据不一致
 * 
 * Queue与其他集合区别：
 * - 相比List：队列专注于FIFO访问，List支持随机访问
 * - 相比Map：队列不需要键，只处理值的排队
 * - 相比Topic：队列确保每条消息只被一个消费者处理，Topic广播到所有订阅者
 * 
 * 性能特点：
 * - 提供高并发的读写支持
 * - 支持高性能的批量操作
 * - 操作的复杂度与队列大小无关
 * 
 * 本示例演示了Hazelcast Queue的基本操作，包括添加、移除、检查、
 * 阻塞操作、超时操作、监听器和批量操作等功能。
 */
@Component
public class QueueBasicOperationsDemo {

  private final HazelcastInstance hazelcastInstance;
  private final String QUEUE_NAME = "demo-queue";

  @Autowired
  public QueueBasicOperationsDemo(HazelcastInstance hazelcastInstance) {
    this.hazelcastInstance = hazelcastInstance;
  }

  /**
   * 运行所有基本操作示例
   */
  public void runAllExamples() {
    System.out.println("=================== Hazelcast Queue 基本操作示例 ===================");

    basicOperationsExample();
    pollAndPeekExample();
    offerWithTimeoutExample();
    drainToExample();
    queueListenerExample();
  }

  /**
   * 基本操作示例
   */
  public void basicOperationsExample() {
    System.out.println("\n--- 基本队列操作示例 ---");

    // 获取分布式队列
    IQueue<String> queue = hazelcastInstance.getQueue(QUEUE_NAME);

    // 清空队列（确保示例从空队列开始）
    queue.clear();
    System.out.println("队列已清空");

    // 添加元素
    queue.add("Item-1");
    queue.add("Item-2");
    queue.add("Item-3");
    System.out.println("已添加3个元素到队列");

    // 展示队列大小
    System.out.println("队列大小: " + queue.size());

    // 检查包含关系
    boolean containsItem2 = queue.contains("Item-2");
    System.out.println("队列包含 'Item-2': " + containsItem2);

    // 移除特定元素
    boolean removed = queue.remove("Item-2");
    System.out.println("已移除 'Item-2': " + removed);
    System.out.println("移除后队列大小: " + queue.size());

    // 获取队列所有元素
    List<String> allItems = new ArrayList<>(queue);
    System.out.println("队列中的所有元素:");
    for (String item : allItems) {
      System.out.println(" - " + item);
    }
  }

  /**
   * poll和peek操作示例
   */
  public void pollAndPeekExample() {
    System.out.println("\n--- Poll和Peek操作示例 ---");

    // 获取分布式队列
    IQueue<String> queue = hazelcastInstance.getQueue(QUEUE_NAME);

    // 清空队列（确保示例从空队列开始）
    queue.clear();

    // 添加测试数据
    queue.add("First");
    queue.add("Second");
    queue.add("Third");
    System.out.println("队列初始化完成，大小: " + queue.size());

    // peek操作 - 查看队首元素但不移除
    String peeked = queue.peek();
    System.out.println("使用peek()查看队首元素: " + peeked);
    System.out.println("peek()后队列大小: " + queue.size());

    // poll操作 - 移除并返回队首元素
    String polled = queue.poll();
    System.out.println("使用poll()移除队首元素: " + polled);
    System.out.println("poll()后队列大小: " + queue.size());

    // 再次peek操作
    peeked = queue.peek();
    System.out.println("再次使用peek()查看队首元素: " + peeked);
  }

  /**
   * 带超时的offer操作示例
   */
  public void offerWithTimeoutExample() {
    System.out.println("\n--- 带超时的Offer操作示例 ---");

    // 获取分布式队列
    IQueue<String> queue = hazelcastInstance.getQueue(QUEUE_NAME);

    // 清空队列
    queue.clear();

    try {
      // 添加元素使用offer方法带超时
      boolean offered1 = queue.offer("OfferItem-1", 1, TimeUnit.SECONDS);
      System.out.println("添加 'OfferItem-1' 结果: " + offered1);

      boolean offered2 = queue.offer("OfferItem-2", 1, TimeUnit.SECONDS);
      System.out.println("添加 'OfferItem-2' 结果: " + offered2);

      System.out.println("队列大小: " + queue.size());

      // 尝试移除元素并设置超时
      String item = queue.poll(2, TimeUnit.SECONDS);
      System.out.println("使用poll(2, TimeUnit.SECONDS)移除元素: " + item);

      // 尝试在空队列上调用poll
      queue.clear();
      System.out.println("队列已清空");

      System.out.println("等待从空队列中poll元素（最多1秒）...");
      String emptyItem = queue.poll(1, TimeUnit.SECONDS);
      System.out.println("poll结果: " + (emptyItem == null ? "超时，未获取到元素" : emptyItem));

    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      System.err.println("操作被中断: " + e.getMessage());
    }
  }

  /**
   * drainTo操作示例 - 将队列中的元素批量导出到集合
   */
  public void drainToExample() {
    System.out.println("\n--- DrainTo操作示例 ---");

    // 获取分布式队列
    IQueue<String> queue = hazelcastInstance.getQueue(QUEUE_NAME);

    // 清空队列
    queue.clear();

    // 添加测试数据
    for (int i = 1; i <= 10; i++) {
      queue.add("Batch-Item-" + i);
    }
    System.out.println("已添加10个元素到队列");

    // 使用drainTo批量获取元素
    List<String> list = new ArrayList<>();
    int drained = queue.drainTo(list, 5); // 最多获取5个元素

    System.out.println("使用drainTo从队列中获取了 " + drained + " 个元素");
    System.out.println("导出的元素:");
    for (String item : list) {
      System.out.println(" - " + item);
    }

    System.out.println("队列中剩余元素: " + queue.size());

    // 再次drainTo获取所有剩余元素
    list.clear();
    drained = queue.drainTo(list); // 获取所有剩余元素
    System.out.println("再次使用drainTo获取了 " + drained + " 个元素");
    System.out.println("队列是否为空: " + queue.isEmpty());
  }

  /**
   * 队列监听器示例
   */
  public void queueListenerExample() {
    System.out.println("\n--- 队列监听器示例 ---");

    // 获取分布式队列
    IQueue<String> queue = hazelcastInstance.getQueue(QUEUE_NAME);

    // 清空队列
    queue.clear();

    // 创建用于等待事件的锁
    CountDownLatch addLatch = new CountDownLatch(3);
    CountDownLatch removeLatch = new CountDownLatch(2);

    // 添加队列项目监听器
    UUID listenerId = queue.addItemListener(new ItemListener<String>() {
      @Override
      public void itemAdded(ItemEvent<String> item) {
        System.out.println("队列项目已添加: " + item.getItem());
        addLatch.countDown();
      }

      @Override
      public void itemRemoved(ItemEvent<String> item) {
        System.out.println("队列项目已移除: " + item.getItem());
        removeLatch.countDown();
      }
    }, true); // includeValue=true表示在事件中包含项目值

    System.out.println("已添加队列监听器，ID: " + listenerId);

    // 添加项目
    queue.add("Event-Item-1");
    queue.add("Event-Item-2");
    queue.add("Event-Item-3");

    try {
      // 等待添加事件
      boolean allAddsReceived = addLatch.await(5, TimeUnit.SECONDS);
      if (!allAddsReceived) {
        System.out.println("警告: 等待接收添加事件超时");
      }

      // 移除项目
      queue.remove("Event-Item-1");
      queue.poll();

      // 等待移除事件
      boolean allRemovesReceived = removeLatch.await(5, TimeUnit.SECONDS);
      if (!allRemovesReceived) {
        System.out.println("警告: 等待接收移除事件超时");
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      System.err.println("等待事件时被中断: " + e.getMessage());
    }

    // 移除监听器
    queue.removeItemListener(listenerId);
    System.out.println("已移除队列监听器");
  }
}