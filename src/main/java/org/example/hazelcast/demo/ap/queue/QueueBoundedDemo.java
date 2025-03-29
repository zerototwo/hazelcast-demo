package org.example.hazelcast.demo.ap.queue;

import com.hazelcast.collection.IQueue;
import com.hazelcast.config.Config;
import com.hazelcast.config.QueueConfig;
import com.hazelcast.core.HazelcastInstance;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Hazelcast 有界队列示例
 */
@Component
public class QueueBoundedDemo {

  private final HazelcastInstance hazelcastInstance;
  private final String BOUNDED_QUEUE_NAME = "bounded-demo-queue";
  private final int MAX_QUEUE_SIZE = 5;

  @Autowired
  public QueueBoundedDemo(HazelcastInstance hazelcastInstance) {
    this.hazelcastInstance = hazelcastInstance;

    // 初始化：配置有界队列
    Config config = hazelcastInstance.getConfig();
    QueueConfig queueConfig = config.getQueueConfig(BOUNDED_QUEUE_NAME);
    queueConfig.setMaxSize(MAX_QUEUE_SIZE);
    System.out.println("已配置有界队列 " + BOUNDED_QUEUE_NAME + " 最大容量为: " + MAX_QUEUE_SIZE);
  }

  /**
   * 运行所有有界队列示例
   */
  public void runAllExamples() {
    System.out.println("=================== Hazelcast 有界队列示例 ===================");

    boundedQueueBasicExample();
    producerConsumerExample();
    offerTimeoutExample();
  }

  /**
   * 有界队列基本示例
   */
  public void boundedQueueBasicExample() {
    System.out.println("\n--- 有界队列基本示例 ---");

    // 获取有界队列
    IQueue<String> boundedQueue = hazelcastInstance.getQueue(BOUNDED_QUEUE_NAME);

    // 清空队列
    boundedQueue.clear();
    System.out.println("队列已清空");

    // 添加元素直到达到限制
    for (int i = 1; i <= MAX_QUEUE_SIZE + 2; i++) {
      String item = "BoundedItem-" + i;
      boolean added = boundedQueue.offer(item);
      if (added) {
        System.out.println("成功添加: " + item);
      } else {
        System.out.println("添加失败，队列已满: " + item);
      }
    }

    System.out.println("队列当前大小: " + boundedQueue.size());
    System.out.println("队列剩余容量: " + boundedQueue.remainingCapacity());

    // 移除一些元素
    System.out.println("\n移除两个元素:");
    System.out.println("移除: " + boundedQueue.poll());
    System.out.println("移除: " + boundedQueue.poll());

    System.out.println("移除后队列大小: " + boundedQueue.size());
    System.out.println("移除后剩余容量: " + boundedQueue.remainingCapacity());

    // 尝试再次添加
    for (int i = MAX_QUEUE_SIZE + 3; i <= MAX_QUEUE_SIZE + 5; i++) {
      String item = "BoundedItem-" + i;
      boolean added = boundedQueue.offer(item);
      System.out.println("添加 " + item + ": " + (added ? "成功" : "失败"));
    }

    System.out.println("最终队列大小: " + boundedQueue.size());
  }

  /**
   * 生产者消费者示例
   */
  public void producerConsumerExample() {
    System.out.println("\n--- 生产者消费者示例 ---");

    // 获取有界队列
    IQueue<String> boundedQueue = hazelcastInstance.getQueue(BOUNDED_QUEUE_NAME);

    // 清空队列
    boundedQueue.clear();

    // 计数器和线程池
    final int itemCount = 20;
    final AtomicInteger producedCount = new AtomicInteger(0);
    final AtomicInteger consumedCount = new AtomicInteger(0);
    final CountDownLatch completionLatch = new CountDownLatch(1);

    ExecutorService executor = Executors.newFixedThreadPool(2);

    // 启动生产者线程
    executor.submit(() -> {
      try {
        System.out.println("生产者启动，将产生 " + itemCount + " 个项目...");

        for (int i = 1; i <= itemCount; i++) {
          String item = "PCItem-" + i;
          boundedQueue.put(item); // 使用阻塞的put方法
          int count = producedCount.incrementAndGet();
          System.out.println("已生产项目: " + item + " (总计: " + count + ")");

          // 生产者速度比消费者快，模拟队列被填满的情况
          if (i % 3 == 0) {
            Thread.sleep(100); // 稍微减缓生产速度
          }
        }

        System.out.println("生产者完成所有项目生产");
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        System.err.println("生产者被中断: " + e.getMessage());
      }
    });

    // 启动消费者线程
    executor.submit(() -> {
      try {
        System.out.println("消费者启动...");

        // 消费直到完成所有项目
        while (consumedCount.get() < itemCount) {
          String item = boundedQueue.take(); // 使用阻塞的take方法
          int count = consumedCount.incrementAndGet();
          System.out.println("已消费项目: " + item + " (总计: " + count + ")");

          // 消费者比生产者慢
          Thread.sleep(300);

          // 显示当前队列状态
          if (count % 5 == 0) {
            System.out.println("队列状态 - 大小: " + boundedQueue.size() +
                ", 剩余容量: " + boundedQueue.remainingCapacity());
          }
        }

        System.out.println("消费者完成所有项目消费");
        completionLatch.countDown();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        System.err.println("消费者被中断: " + e.getMessage());
      }
    });

    try {
      // 等待示例完成
      boolean completed = completionLatch.await(30, TimeUnit.SECONDS);
      if (!completed) {
        System.out.println("警告: 等待生产者/消费者示例完成超时");
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      System.err.println("等待示例完成时被中断: " + e.getMessage());
    }

    executor.shutdownNow();
    System.out.println("生产者/消费者示例结束 - 队列大小: " + boundedQueue.size());
  }

  /**
   * offer超时示例
   */
  public void offerTimeoutExample() {
    System.out.println("\n--- Offer超时示例 ---");

    // 获取有界队列
    IQueue<String> boundedQueue = hazelcastInstance.getQueue(BOUNDED_QUEUE_NAME);

    // 清空队列
    boundedQueue.clear();

    try {
      // 添加元素直到队列满
      for (int i = 1; i <= MAX_QUEUE_SIZE; i++) {
        boundedQueue.put("TimeoutItem-" + i);
      }
      System.out.println("队列已填满，大小: " + boundedQueue.size());

      // 尝试添加另一个元素，使用offer带超时
      System.out.println("尝试添加另一个元素，等待最多3秒...");
      long startTime = System.currentTimeMillis();
      boolean added = boundedQueue.offer("TimeoutItem-Overflow", 3, TimeUnit.SECONDS);
      long endTime = System.currentTimeMillis();

      System.out.println("添加结果: " + (added ? "成功" : "超时") +
          " (耗时: " + (endTime - startTime) + "ms)");

      // 创建一个单独的线程来消费，以便下一次offer可以成功
      Thread consumerThread = new Thread(() -> {
        try {
          Thread.sleep(1000); // 等待1秒再消费
          String consumed = boundedQueue.poll();
          System.out.println("消费者移除了元素: " + consumed);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      });
      consumerThread.start();

      // 再次尝试添加，这次应该会成功
      System.out.println("再次尝试添加，等待最多3秒...");
      startTime = System.currentTimeMillis();
      added = boundedQueue.offer("TimeoutItem-New", 3, TimeUnit.SECONDS);
      endTime = System.currentTimeMillis();

      System.out.println("添加结果: " + (added ? "成功" : "超时") +
          " (耗时: " + (endTime - startTime) + "ms)");

      consumerThread.join(); // 等待消费者线程完成

    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      System.err.println("操作被中断: " + e.getMessage());
    }

    System.out.println("最终队列状态 - 大小: " + boundedQueue.size());
  }
}