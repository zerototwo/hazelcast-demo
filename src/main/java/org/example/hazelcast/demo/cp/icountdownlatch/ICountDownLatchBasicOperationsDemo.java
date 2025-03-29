package org.example.hazelcast.demo.cp.icountdownlatch;

import com.hazelcast.core.HazelcastInstance;
// 注意: 以下导入在社区版中不可用，仅在企业版中可用
// import com.hazelcast.cp.ICountDownLatch;
// import com.hazelcast.cp.CPSubsystem;
import org.springframework.stereotype.Component;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * ICountDownLatch基本操作示例
 * <p>
 * ICountDownLatch是java.util.concurrent.CountDownLatch的分布式实现，
 * 提供了在分布式环境中协调活动的机制。与Java的实现不同，Hazelcast的ICountDownLatch在
 * 计数结束后可以重置计数器，但不能在活动计数期间重置。
 * </p>
 * 
 * <p>
 * <strong>ICountDownLatch的主要特性：</strong>
 * </p>
 * <ul>
 * <li><strong>分布式同步：</strong> 在集群中的不同节点之间提供同步机制</li>
 * <li><strong>线性一致性：</strong> 作为CP数据结构，提供强一致性保证</li>
 * <li><strong>可重置：</strong> 计数完成后可以重新设置计数值</li>
 * <li><strong>等待超时：</strong> 支持带超时的等待操作</li>
 * <li><strong>集群范围：</strong> 在整个集群范围内协调活动</li>
 * </ul>
 * 
 * <p>
 * <strong>适用场景：</strong>
 * </p>
 * <ul>
 * <li>分布式任务的启动信号 - 等待所有准备工作完成后再开始</li>
 * <li>分布式任务的完成信号 - 等待所有子任务完成</li>
 * <li>主从协调 - leader/follower模式中协调活动</li>
 * <li>系统初始化同步 - 等待系统组件初始化完成</li>
 * <li>批处理作业协调 - 协调多节点批处理操作</li>
 * </ul>
 * 
 * <p>
 * <strong>使用注意事项：</strong>
 * </p>
 * <ul>
 * <li>不能在活动计数期间重置计数器，只能在计数完成后重置</li>
 * <li>计数器不会自动销毁，需要在不再使用时手动销毁</li>
 * <li>await方法支持超时，可防止永久阻塞</li>
 * <li>作为CP数据结构，操作涉及网络通信，性能与本地CountDownLatch不同</li>
 * </ul>
 * 
 * <p>
 * 注意: ICountDownLatch作为CP数据结构，在社区版中有限支持，完整功能在企业版中可用。
 * 本示例代码使用模拟实现，主要用于演示API用法。
 * </p>
 */
@Component
public class ICountDownLatchBasicOperationsDemo {

  private final HazelcastInstance hazelcastInstance;

  public ICountDownLatchBasicOperationsDemo(HazelcastInstance hazelcastInstance) {
    this.hazelcastInstance = hazelcastInstance;
  }

  /**
   * 运行所有ICountDownLatch示例
   * 
   * <p>
   * 依次展示ICountDownLatch的各项功能，包括基本操作、主从模式示例和
   * 并行任务协调示例，全面展示其API和使用场景。
   * </p>
   */
  public void runAllExamples() {
    System.out.println("\n==== ICountDownLatch基本操作示例 ====");
    basicOperationsExample();
    leaderFollowerExample();
    parallelTasksExample();
    System.out.println("==== ICountDownLatch示例结束 ====\n");
  }

  /**
   * 演示ICountDownLatch的基本操作
   * 
   * <p>
   * 展示如何执行以下操作：
   * </p>
   * <ul>
   * <li>创建和获取ICountDownLatch实例</li>
   * <li>设置计数器初始值 (trySetCount)</li>
   * <li>减少计数器的值 (countDown)</li>
   * <li>等待计数器归零 (await)</li>
   * <li>带超时的等待 (await with timeout)</li>
   * </ul>
   * 
   * <p>
   * 这些基本操作是构建更复杂协调逻辑的基础。
   * </p>
   */
  public void basicOperationsExample() {
    System.out.println("\n-- ICountDownLatch基本操作 --");
    System.out.println("模拟ICountDownLatch操作，实际应使用CP子系统获取");

    // 实际使用时：
    // ICountDownLatch latch =
    // hazelcastInstance.getCPSubsystem().getCountDownLatch("myLatch");

    // 创建模拟的CountDownLatch
    CountDownLatch mockLatch = new CountDownLatch(3);

    System.out.println("设置计数器初始值: 3");
    // 实际ICountDownLatch支持trySetCount方法重置计数

    System.out.println("当前计数: " + mockLatch.getCount());

    System.out.println("执行countDown...");
    mockLatch.countDown();
    System.out.println("当前计数: " + mockLatch.getCount());

    System.out.println("再次执行countDown...");
    mockLatch.countDown();
    System.out.println("当前计数: " + mockLatch.getCount());

    System.out.println("最后一次执行countDown...");
    mockLatch.countDown();
    System.out.println("当前计数: " + mockLatch.getCount());

    System.out.println("计数器已归零，await立即返回");

    // 当计数完成后，ICountDownLatch允许重置计数器
    System.out.println("计数完成后，ICountDownLatch允许重置计数器 (trySetCount)");

    // 使用带超时的await
    System.out.println("演示带超时的await (注：超时为5秒)");
    CountDownLatch timeoutLatch = new CountDownLatch(1);

    try {
      boolean completed = timeoutLatch.await(2, TimeUnit.SECONDS);
      System.out.println("await超时，返回: " + completed);
    } catch (InterruptedException e) {
      System.err.println("等待被中断: " + e.getMessage());
    }

    System.out.println("注意: 不使用时应调用destroy()方法销毁ICountDownLatch实例");
  }

  /**
   * 演示ICountDownLatch在领导者-追随者模式中的应用
   * 
   * <p>
   * 展示领导者-追随者模式，其中:
   * </p>
   * <ul>
   * <li>领导者初始化工作，然后发出信号</li>
   * <li>追随者等待领导者的信号，然后开始自己的工作</li>
   * </ul>
   * 
   * <p>
   * 这种模式常用于需要首先完成某些初始化操作，然后允许其他操作继续的场景。
   * 例如系统启动序列、主从架构中的协调、批处理作业的启动等。
   * </p>
   */
  public void leaderFollowerExample() {
    System.out.println("\n-- ICountDownLatch领导者-追随者示例 --");

    // 创建模拟的CountDownLatch
    CountDownLatch mockLatch = new CountDownLatch(1);
    int followerCount = 3;

    ExecutorService executor = Executors.newFixedThreadPool(followerCount + 1);

    // 启动领导者线程
    executor.submit(() -> {
      try {
        System.out.println("领导者: 开始初始化工作...");
        Thread.sleep(2000); // 模拟工作
        System.out.println("领导者: 工作完成，通知追随者");
        mockLatch.countDown();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        System.err.println("领导者线程被中断: " + e.getMessage());
      }
    });

    // 启动追随者线程
    for (int i = 0; i < followerCount; i++) {
      final int followerId = i + 1;
      executor.submit(() -> {
        try {
          System.out.println("追随者-" + followerId + ": 等待领导者信号...");
          mockLatch.await();
          System.out.println("追随者-" + followerId + ": 收到信号，开始工作");
          // 模拟工作
          Thread.sleep(1000);
          System.out.println("追随者-" + followerId + ": 完成工作");
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          System.err.println("追随者线程被中断: " + e.getMessage());
        }
      });
    }

    // 关闭线程池
    executor.shutdown();
    try {
      // 等待所有任务完成
      executor.awaitTermination(10, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      System.err.println("等待线程池关闭时被中断: " + e.getMessage());
    }

    System.out.println("领导者-追随者示例完成");
  }

  /**
   * 演示ICountDownLatch在并行任务协调中的应用
   * 
   * <p>
   * 展示如何使用ICountDownLatch协调多个并行任务的完成，其中:
   * </p>
   * <ul>
   * <li>主线程创建多个并行任务</li>
   * <li>每个任务完成后减少计数器</li>
   * <li>主线程等待所有任务完成（计数器归零）</li>
   * <li>然后执行最终处理</li>
   * </ul>
   * 
   * <p>
   * 这种模式常用于分布式计算、并行数据处理、分布式系统初始化等场景，
   * 需要等待多个异步操作完成后再继续。
   * </p>
   */
  public void parallelTasksExample() {
    System.out.println("\n-- ICountDownLatch并行任务协调示例 --");

    int taskCount = 5;
    CountDownLatch mockLatch = new CountDownLatch(taskCount);
    ExecutorService executor = Executors.newFixedThreadPool(taskCount);

    System.out.println("主线程: 启动" + taskCount + "个并行任务");

    for (int i = 0; i < taskCount; i++) {
      final int taskId = i + 1;
      executor.submit(() -> {
        try {
          System.out.println("任务-" + taskId + ": 开始执行");
          // 模拟不同的执行时间
          Thread.sleep(1000 + (int) (Math.random() * 2000));
          System.out.println("任务-" + taskId + ": 执行完成");
          mockLatch.countDown();
          System.out.println("任务-" + taskId + ": 减少计数器，当前剩余: " + mockLatch.getCount());
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          System.err.println("任务线程被中断: " + e.getMessage());
        }
      });
    }

    System.out.println("主线程: 等待所有任务完成...");

    try {
      boolean allCompleted = mockLatch.await(10, TimeUnit.SECONDS);
      if (allCompleted) {
        System.out.println("主线程: 所有任务已完成，执行最终处理");
        // 这里可以执行所有任务完成后的操作
      } else {
        System.out.println("主线程: 等待超时，部分任务未完成");
      }
    } catch (InterruptedException e) {
      System.err.println("主线程等待被中断: " + e.getMessage());
    }

    // 关闭线程池
    executor.shutdown();
    try {
      executor.awaitTermination(5, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      System.err.println("等待线程池关闭时被中断: " + e.getMessage());
    }

    System.out.println("并行任务协调示例完成");
  }

  /**
   * 演示TrySetCount操作
   * 
   * <p>
   * 这个方法演示了ICountDownLatch的trySetCount操作特性：
   * </p>
   * <ul>
   * <li>当计数器为0时，可以重新设置计数</li>
   * <li>当计数器不为0时，设置计数会失败</li>
   * </ul>
   * 
   * <p>
   * 此特性与Java的CountDownLatch不同，Java版本不允许重置计数。
   * Hazelcast的ICountDownLatch允许在计数完成后重置计数，但不能在活动计数期间重置。
   * </p>
   */
  public void trySetCountExample() {
    System.out.println("\n-- ICountDownLatch trySetCount示例 --");

    // 创建一个初始计数为2的CountDownLatch
    CountDownLatch mockLatch = new CountDownLatch(2);

    System.out.println("初始计数: " + mockLatch.getCount());
    System.out.println("计数器不为0时，trySetCount会失败");

    // 减少计数
    System.out.println("执行countDown...");
    mockLatch.countDown();
    System.out.println("当前计数: " + mockLatch.getCount());

    System.out.println("计数器仍不为0，trySetCount会失败");

    // 再次减少计数
    System.out.println("再次执行countDown...");
    mockLatch.countDown();
    System.out.println("当前计数: " + mockLatch.getCount());

    System.out.println("计数器现在为0，trySetCount可以成功");

    // 实际ICountDownLatch支持trySetCount操作
    System.out.println("在实际的ICountDownLatch中，我们现在可以重置计数，例如:");
    System.out.println("latch.trySetCount(3) 会返回true，并将计数重置为3");

    System.out.println("注意：这是Hazelcast ICountDownLatch与Java标准CountDownLatch的一个主要区别");
  }
}