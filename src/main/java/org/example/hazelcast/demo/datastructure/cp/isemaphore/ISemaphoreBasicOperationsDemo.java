package org.example.hazelcast.demo.datastructure.cp.isemaphore;

import com.hazelcast.core.HazelcastInstance;
// 注意: 以下导入在社区版中不可用，仅在企业版中可用
// import com.hazelcast.cp.ISemaphore;
// import com.hazelcast.cp.CPSubsystem;
import org.springframework.stereotype.Component;

import java.util.concurrent.Semaphore;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * ISemaphore基本操作示例
 * <p>
 * ISemaphore是java.util.concurrent.Semaphore的分布式实现，
 * 提供了在分布式环境中控制并发活动的机制。Semaphore通过"许可证"(permits)
 * 来限制对共享资源的同时访问线程数。
 * </p>
 * 
 * <p>
 * <strong>ISemaphore的主要特性：</strong>
 * </p>
 * <ul>
 * <li><strong>并发控制：</strong> 限制访问共享资源的并发线程数</li>
 * <li><strong>线性一致性：</strong> 作为CP数据结构，提供强一致性保证</li>
 * <li><strong>分布式信号量：</strong> 在集群范围内协调并发访问</li>
 * <li><strong>可配置许可数：</strong> 可以设置初始许可证数量</li>
 * <li><strong>支持超时：</strong> 获取许可证的操作支持超时设置</li>
 * </ul>
 * 
 * <p>
 * <strong>适用场景：</strong>
 * </p>
 * <ul>
 * <li>限制对共享资源的并发访问 - 例如数据库连接池</li>
 * <li>控制服务负载 - 限制同时处理的请求数</li>
 * <li>分布式环境中的速率限制</li>
 * <li>资源池管理 - 例如线程池、连接池等</li>
 * <li>实现更复杂的并发控制模式</li>
 * </ul>
 * 
 * <p>
 * <strong>使用注意事项：</strong>
 * </p>
 * <ul>
 * <li>单许可证的ISemaphore可视为分布式锁，但任何线程都可以释放许可证</li>
 * <li>不保证完全的公平性，某些边缘情况下可能会出现非公平调度</li>
 * <li>不使用时需手动销毁，避免内存泄漏</li>
 * <li>获取许可证的操作会阻塞线程，可以使用tryAcquire设置超时时间</li>
 * </ul>
 * 
 * <p>
 * 注意: ISemaphore作为CP数据结构，在社区版中有限支持，完整功能在企业版中可用。
 * 本示例代码使用模拟实现，主要用于演示API用法。
 * </p>
 */
@Component
public class ISemaphoreBasicOperationsDemo {

  private final HazelcastInstance hazelcastInstance;

  public ISemaphoreBasicOperationsDemo(HazelcastInstance hazelcastInstance) {
    this.hazelcastInstance = hazelcastInstance;
  }

  /**
   * 运行所有ISemaphore示例
   * 
   * <p>
   * 依次展示ISemaphore的各项功能，包括基本操作、资源限制示例和
   * 多次获取释放示例，全面展示其API和使用场景。
   * </p>
   */
  public void runAllExamples() {
    System.out.println("\n==== ISemaphore基本操作示例 ====");
    basicOperationsExample();
    resourceLimitExample();
    multiPermitsExample();
    timeoutExample();
    fairnessExample();
    System.out.println("==== ISemaphore示例结束 ====\n");
  }

  /**
   * 演示ISemaphore的基本操作
   * 
   * <p>
   * 展示如何执行以下操作：
   * </p>
   * <ul>
   * <li>创建和获取ISemaphore实例</li>
   * <li>初始化许可证数量 (init)</li>
   * <li>获取许可证 (acquire)</li>
   * <li>释放许可证 (release)</li>
   * <li>尝试获取许可证 (tryAcquire)</li>
   * <li>获取当前可用许可证数 (availablePermits)</li>
   * </ul>
   * 
   * <p>
   * 这些基本操作是使用ISemaphore进行并发控制的基础。
   * </p>
   */
  public void basicOperationsExample() {
    System.out.println("\n-- ISemaphore基本操作 --");
    System.out.println("模拟ISemaphore操作，实际应使用CP子系统获取");

    // 实际使用时：
    // ISemaphore semaphore =
    // hazelcastInstance.getCPSubsystem().getSemaphore("mySemaphore");
    // semaphore.init(5); // 初始化为5个许可证

    // 创建模拟的Semaphore，初始5个许可证
    Semaphore mockSemaphore = new Semaphore(5, true);

    System.out.println("创建信号量，初始化5个许可证");
    System.out.println("当前可用许可证数: " + mockSemaphore.availablePermits());

    try {
      System.out.println("尝试获取1个许可证...");
      mockSemaphore.acquire();
      System.out.println("成功获取1个许可证");
      System.out.println("当前可用许可证数: " + mockSemaphore.availablePermits());

      System.out.println("尝试获取2个许可证...");
      mockSemaphore.acquire(2);
      System.out.println("成功获取2个许可证");
      System.out.println("当前可用许可证数: " + mockSemaphore.availablePermits());

      System.out.println("尝试使用tryAcquire获取1个许可证...");
      boolean acquired = mockSemaphore.tryAcquire();
      System.out.println("tryAcquire结果: " + acquired);
      System.out.println("当前可用许可证数: " + mockSemaphore.availablePermits());

      System.out.println("尝试使用tryAcquire获取2个许可证（不足）...");
      acquired = mockSemaphore.tryAcquire(2);
      System.out.println("tryAcquire结果: " + acquired);
      System.out.println("当前可用许可证数: " + mockSemaphore.availablePermits());

      System.out.println("释放3个许可证...");
      mockSemaphore.release(3);
      System.out.println("当前可用许可证数: " + mockSemaphore.availablePermits());

      System.out.println("释放剩余许可证...");
      mockSemaphore.release(2);
      System.out.println("当前可用许可证数: " + mockSemaphore.availablePermits());

    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      System.err.println("获取许可证时被中断: " + e.getMessage());
    }

    System.out.println("注意: 不使用时应调用destroy()方法销毁ISemaphore实例");
  }

  /**
   * 演示ISemaphore如何限制对共享资源的并发访问
   * 
   * <p>
   * 此示例展示了ISemaphore的典型用例 - 限制对共享资源的并发访问：
   * </p>
   * <ul>
   * <li>创建一个有限许可证的信号量</li>
   * <li>多个线程尝试同时访问受保护的资源</li>
   * <li>信号量确保在任何时刻只有有限数量的线程可以访问资源</li>
   * <li>当线程完成访问后，释放许可证，允许其他等待的线程访问</li>
   * </ul>
   * 
   * <p>
   * 这种模式在实际应用中非常常见，例如限制数据库连接数、
   * 控制网络请求数、管理资源池等。
   * </p>
   */
  public void resourceLimitExample() {
    System.out.println("\n-- 资源限制示例 --");

    // 模拟资源，最多允许3个线程同时访问
    Semaphore mockSemaphore = new Semaphore(3, true);
    // 用于跟踪当前活跃线程数
    AtomicLong activeThreads = new AtomicLong(0);
    // 用于记录最大并发访问数
    AtomicLong maxConcurrent = new AtomicLong(0);

    int threadCount = 10;
    ExecutorService executor = Executors.newFixedThreadPool(threadCount);

    System.out.println("创建信号量，限制最多3个线程同时访问资源");
    System.out.println("启动10个线程尝试访问资源...");

    for (int i = 0; i < threadCount; i++) {
      final int threadId = i + 1;
      executor.submit(() -> {
        try {
          System.out.println("线程-" + threadId + ": 尝试获取许可证...");
          mockSemaphore.acquire();

          try {
            // 增加活跃线程计数
            long currentActive = activeThreads.incrementAndGet();
            // 更新最大并发数
            updateMaxConcurrent(currentActive, maxConcurrent);

            System.out.println("线程-" + threadId + ": 获取许可证成功，开始访问资源。当前活跃线程: " + currentActive);

            // 模拟资源访问时间
            Thread.sleep(1000 + (int) (Math.random() * 1000));

          } finally {
            // 减少活跃线程计数并释放许可证
            System.out.println("线程-" + threadId + ": 完成资源访问，释放许可证。当前活跃线程: " +
                (activeThreads.decrementAndGet()));
            mockSemaphore.release();
          }

        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          System.err.println("线程-" + threadId + " 被中断: " + e.getMessage());
        }
      });
    }

    // 关闭线程池，等待所有任务完成
    executor.shutdown();
    try {
      executor.awaitTermination(15, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      System.err.println("等待线程池关闭时被中断: " + e.getMessage());
    }

    System.out.println("所有线程都已完成资源访问");
    System.out.println("最大并发访问数: " + maxConcurrent.get());
    System.out.println("验证资源限制是否有效: " + (maxConcurrent.get() <= 3 ? "是" : "否"));
  }

  /**
   * 辅助方法：更新最大并发数
   */
  private void updateMaxConcurrent(long currentActive, AtomicLong maxConcurrent) {
    long current;
    while (currentActive > (current = maxConcurrent.get())) {
      maxConcurrent.compareAndSet(current, currentActive);
    }
  }

  /**
   * 演示获取和释放多个许可证
   * 
   * <p>
   * 此示例展示了如何一次性获取和释放多个许可证：
   * </p>
   * <ul>
   * <li>创建一个具有多个许可证的信号量</li>
   * <li>展示如何一次性获取多个许可证</li>
   * <li>展示如何一次性释放多个许可证</li>
   * </ul>
   * 
   * <p>
   * 这种能力在需要控制资源使用量的场景中很有用，
   * 例如，一个任务可能需要多个资源单元才能执行。
   * </p>
   */
  public void multiPermitsExample() {
    System.out.println("\n-- 多许可证获取和释放示例 --");

    // 创建一个有10个许可证的信号量
    Semaphore mockSemaphore = new Semaphore(10, true);

    System.out.println("创建信号量，初始化10个许可证");
    System.out.println("当前可用许可证数: " + mockSemaphore.availablePermits());

    try {
      // 一次性获取不同数量的许可证
      System.out.println("线程A: 尝试获取2个许可证...");
      mockSemaphore.acquire(2);
      System.out.println("线程A: 成功获取2个许可证，当前可用: " + mockSemaphore.availablePermits());

      System.out.println("线程B: 尝试获取3个许可证...");
      mockSemaphore.acquire(3);
      System.out.println("线程B: 成功获取3个许可证，当前可用: " + mockSemaphore.availablePermits());

      System.out.println("线程C: 尝试获取4个许可证...");
      mockSemaphore.acquire(4);
      System.out.println("线程C: 成功获取4个许可证，当前可用: " + mockSemaphore.availablePermits());

      // 尝试获取超过可用数量的许可证
      System.out.println("线程D: 尝试获取2个许可证（当前只有1个可用）...");

      // 创建一个单独的线程来模拟阻塞获取
      Thread threadD = new Thread(() -> {
        try {
          System.out.println("线程D: 开始等待足够的许可证...");
          mockSemaphore.acquire(2);
          System.out.println("线程D: 成功获取2个许可证，当前可用: " + mockSemaphore.availablePermits());
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          System.out.println("线程D: 等待被中断");
        }
      });
      threadD.start();

      // 给线程D一些时间开始等待
      Thread.sleep(1000);

      // 释放部分许可证
      System.out.println("线程A: 释放2个许可证");
      mockSemaphore.release(2);
      System.out.println("释放后，当前可用: " + mockSemaphore.availablePermits());

      // 等待线程D完成
      threadD.join(2000);

      // 释放所有剩余的许可证
      System.out.println("释放所有剩余的许可证...");
      mockSemaphore.release(9); // 3 + 4 + 2
      System.out.println("当前可用许可证数: " + mockSemaphore.availablePermits());

    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      System.err.println("获取许可证时被中断: " + e.getMessage());
    }
  }

  /**
   * 演示带超时的许可证获取
   * 
   * <p>
   * 此示例展示了如何使用tryAcquire方法设置获取许可证的超时时间：
   * </p>
   * <ul>
   * <li>尝试在指定的时间内获取许可证</li>
   * <li>如果在超时时间内无法获取许可证，则返回false</li>
   * <li>这种方式可以避免线程无限期地阻塞</li>
   * </ul>
   * 
   * <p>
   * 在实际应用中，超时机制可以防止死锁，
   * 并允许系统在资源不可用时执行备选操作。
   * </p>
   */
  public void timeoutExample() {
    System.out.println("\n-- 带超时的许可证获取示例 --");

    // 创建一个只有1个许可证的信号量
    Semaphore mockSemaphore = new Semaphore(1, true);

    System.out.println("创建信号量，只有1个许可证");

    try {
      // 首先获取唯一的许可证
      System.out.println("线程A: 获取唯一的许可证");
      mockSemaphore.acquire();
      System.out.println("线程A: 成功获取许可证，当前可用: " + mockSemaphore.availablePermits());

      // 另一个线程尝试带超时地获取许可证
      System.out.println("线程B: 尝试获取许可证，超时时间为2秒...");
      boolean acquired = mockSemaphore.tryAcquire(2, TimeUnit.SECONDS);
      System.out.println("线程B: 获取结果: " + acquired);

      // 释放许可证
      System.out.println("线程A: 释放许可证");
      mockSemaphore.release();
      System.out.println("当前可用许可证数: " + mockSemaphore.availablePermits());

      // 再次尝试带超时地获取许可证
      System.out.println("线程B: 再次尝试获取许可证，超时时间为2秒...");
      acquired = mockSemaphore.tryAcquire(2, TimeUnit.SECONDS);
      System.out.println("线程B: 获取结果: " + acquired);

      if (acquired) {
        mockSemaphore.release();
      }

    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      System.err.println("获取许可证时被中断: " + e.getMessage());
    }
  }

  /**
   * 演示公平与非公平信号量的差异
   * 
   * <p>
   * 此示例展示了公平与非公平调度的区别：
   * </p>
   * <ul>
   * <li>公平模式下，许可证按请求顺序分配给等待的线程</li>
   * <li>非公平模式下，当许可证可用时，任何等待的线程都可能获取它</li>
   * <li>Hazelcast ISemaphore并不保证完全的公平性，某些情况下可能会出现非公平调度</li>
   * </ul>
   * 
   * <p>
   * 注意：此示例主要是为了解释概念，实际效果可能因运行环境而异。
   * </p>
   */
  public void fairnessExample() {
    System.out.println("\n-- 信号量公平性示例 --");

    // 创建公平信号量
    Semaphore fairSemaphore = new Semaphore(1, true);
    System.out.println("创建公平信号量，初始1个许可证");

    // 创建非公平信号量
    Semaphore unfairSemaphore = new Semaphore(1, false);
    System.out.println("创建非公平信号量，初始1个许可证");

    System.out.println("\n注意：在实际的Hazelcast ISemaphore中，并不保证完全的公平性");
    System.out.println("某些边缘情况下，如当许可证在内部超时时刻变得可用时，可能会出现非公平调度");
    System.out.println("此示例主要是为了解释公平与非公平调度的概念差异\n");

    System.out.println("公平模式下，线程按请求顺序获取许可证");
    System.out.println("非公平模式下，当许可证可用时，任何等待的线程都可能获取它");

    System.out.println("\n以下是一个理论上的执行流程示例：");
    System.out.println("1. 线程A获取了唯一的许可证");
    System.out.println("2. 线程B、C、D按顺序请求许可证，开始等待");
    System.out.println("3. 线程A释放许可证");
    System.out.println("4. 在公平模式下，线程B获取许可证（因为它是最先等待的）");
    System.out.println("5. 在非公平模式下，线程B、C或D中的任何一个都可能获取许可证");

    System.out.println("\nHazelcast ISemaphore的使用建议：");
    System.out.println("- 如果顺序很重要，不要依赖ISemaphore的公平性");
    System.out.println("- 考虑使用其他机制（如队列）来确保请求的有序处理");
    System.out.println("- 设计系统时考虑到可能的非公平性，确保系统不会因此而出现问题");
  }
}