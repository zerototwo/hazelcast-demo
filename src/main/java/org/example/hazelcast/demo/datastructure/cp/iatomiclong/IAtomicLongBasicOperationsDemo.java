package org.example.hazelcast.demo.datastructure.cp.iatomiclong;

import com.hazelcast.core.HazelcastInstance;
// 注意: 以下导入在社区版中不可用，仅在企业版中可用
// import com.hazelcast.cp.IAtomicLong;
// import com.hazelcast.cp.CPSubsystem;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

/**
 * IAtomicLong基本操作示例
 * <p>
 * IAtomicLong是java.util.concurrent.atomic.AtomicLong的分布式实现，
 * 提供了线性一致性的原子操作，支持get、set、getAndSet、compareAndSet、incrementAndGet等方法。
 * </p>
 * 
 * <p>
 * <strong>IAtomicLong的主要特性：</strong>
 * </p>
 * <ul>
 * <li><strong>线性一致性：</strong> 在分布式环境中提供强一致性保证</li>
 * <li><strong>原子操作：</strong> 支持自增、自减等数值操作的原子性</li>
 * <li><strong>CP数据结构：</strong> 作为CP数据结构，支持linearizable读和写操作</li>
 * <li><strong>函数式操作：</strong> 支持apply、alter等函数式方法，避免数据竞争</li>
 * <li><strong>分布式协调：</strong> 可用于集群中的协调和同步</li>
 * </ul>
 * 
 * <p>
 * <strong>适用场景：</strong>
 * </p>
 * <ul>
 * <li>分布式计数器实现</li>
 * <li>序列号生成器</li>
 * <li>分布式速率限制器</li>
 * <li>集群范围内的统计数据收集</li>
 * <li>分布式锁的实现基础</li>
 * <li>集群中的资源协调</li>
 * </ul>
 * 
 * <p>
 * <strong>使用注意事项：</strong>
 * </p>
 * <ul>
 * <li>分布式操作涉及网络通信，性能与本地AtomicLong不同</li>
 * <li>发送函数到数据比从远程获取数据更高效</li>
 * <li>不使用时需手动销毁，避免内存泄漏</li>
 * <li>适合低到中等频率的操作，高频操作可能影响性能</li>
 * </ul>
 * 
 * <p>
 * 注意: IAtomicLong功能作为CP数据结构，在社区版中有限支持，完整功能在企业版中可用。
 * 本示例代码使用模拟实现，主要用于演示API用法。
 * </p>
 */
@Component
public class IAtomicLongBasicOperationsDemo {

  private final HazelcastInstance hazelcastInstance;
  // 用于模拟IAtomicLong的本地计数器
  private final AtomicLong mockAtomicLong = new AtomicLong(0);

  public IAtomicLongBasicOperationsDemo(HazelcastInstance hazelcastInstance) {
    this.hazelcastInstance = hazelcastInstance;
  }

  /**
   * 运行所有IAtomicLong示例
   * 
   * <p>
   * 依次展示IAtomicLong的各项功能，包括基本操作、计数器操作、
   * 函数应用和多线程场景，全面展示其API和使用方法。
   * </p>
   */
  public void runAllExamples() {
    System.out.println("\n==== IAtomicLong基本操作示例 ====");
    basicOperationsExample();
    incrementCounterExample();
    applyFunctionsExample();
    concurrentIncrementExample();
    System.out.println("==== IAtomicLong示例结束 ====\n");
  }

  /**
   * 演示IAtomicLong的基本操作：get、set、getAndSet
   * 
   * <p>
   * 展示如何执行以下操作：
   * </p>
   * <ul>
   * <li>创建和获取IAtomicLong实例</li>
   * <li>获取当前值 (get)</li>
   * <li>设置新值 (set)</li>
   * <li>原子地获取当前值并设置新值 (getAndSet)</li>
   * <li>原子地比较当前值并设置新值 (compareAndSet)</li>
   * </ul>
   * 
   * <p>
   * 这些基本操作是分布式计数器和同步工具的基础，在集群环境中保证操作的原子性。
   * </p>
   */
  public void basicOperationsExample() {
    System.out.println("\n-- IAtomicLong基本操作 --");
    System.out.println("模拟IAtomicLong操作，实际应使用CP子系统获取");

    // 实际使用时：
    // IAtomicLong atomicLong =
    // hazelcastInstance.getCPSubsystem().getAtomicLong("myAtomicLong");

    // 重置模拟计数器
    mockAtomicLong.set(0);
    System.out.println("初始值: " + mockAtomicLong.get());

    // 设置新值
    mockAtomicLong.set(100);
    System.out.println("设置后的值: " + mockAtomicLong.get());

    // 获取并设置
    long oldValue = mockAtomicLong.getAndSet(200);
    System.out.println("getAndSet操作 - 旧值: " + oldValue + ", 新值: " + mockAtomicLong.get());

    // 比较并设置
    boolean success = mockAtomicLong.compareAndSet(200, 300);
    System.out.println("compareAndSet操作 - 成功: " + success + ", 当前值: " + mockAtomicLong.get());

    // 尝试失败的比较并设置
    success = mockAtomicLong.compareAndSet(500, 600);
    System.out.println("失败的compareAndSet - 成功: " + success + ", 当前值: " + mockAtomicLong.get());
  }

  /**
   * 演示使用IAtomicLong实现分布式计数器
   * 
   * <p>
   * 展示IAtomicLong作为分布式计数器的核心功能：
   * </p>
   * <ul>
   * <li>递增操作 (incrementAndGet) - 原子地将值加1并返回新值</li>
   * <li>递减操作 (decrementAndGet) - 原子地将值减1并返回新值</li>
   * </ul>
   * 
   * <p>
   * 分布式计数器是IAtomicLong最常见的应用场景之一，可用于：
   * </p>
   * <ul>
   * <li>跟踪应用程序中的事件计数</li>
   * <li>实现分布式序列号生成</li>
   * <li>为分布式ID生成提供基础</li>
   * <li>实现简单的限流器</li>
   * </ul>
   */
  public void incrementCounterExample() {
    System.out.println("\n-- IAtomicLong作为分布式计数器 --");

    // 重置模拟计数器
    mockAtomicLong.set(0);

    System.out.println("开始递增计数器...");

    // 模拟多次递增操作
    for (int i = 0; i < 10; i++) {
      long newValue = mockAtomicLong.incrementAndGet();
      System.out.println("递增后的值: " + newValue);
    }

    // 递减操作
    for (int i = 0; i < 3; i++) {
      long newValue = mockAtomicLong.decrementAndGet();
      System.out.println("递减后的值: " + newValue);
    }

    System.out.println("最终计数器值: " + mockAtomicLong.get());
  }

  /**
   * 演示IAtomicLong的函数应用
   * <p>
   * 展示apply、alter、alterAndGet和getAndAlter方法，这些方法接受函数作为参数，
   * 使用函数式编程方式操作存储的值。
   * </p>
   * 
   * <p>
   * <strong>函数应用的优势：</strong>
   * </p>
   * <ul>
   * <li><strong>性能优化：</strong> 将函数发送到数据而不是将数据取回，减少网络传输</li>
   * <li><strong>避免竞态条件：</strong> 不需要执行读取-修改-写入等多步操作，减少并发问题</li>
   * <li><strong>复杂计算：</strong> 可以执行复杂的计算逻辑，不仅限于简单的增减操作</li>
   * </ul>
   * 
   * <p>
   * 在实际Hazelcast中，需要实现IFunction接口，本示例使用Java的Function模拟。
   * </p>
   */
  public void applyFunctionsExample() {
    System.out.println("\n-- IAtomicLong的函数应用 --");

    // 实际Hazelcast中，我们使用IFunction接口
    // 这里我们使用Java标准库中的Function作为替代

    // 重置模拟计数器
    mockAtomicLong.set(1);
    System.out.println("初始值: " + mockAtomicLong.get());

    // 定义一个简单的函数，增加2
    Function<Long, Long> add2Function = input -> input + 2;

    // 模拟apply - 应用函数但不改变原值
    long result = add2Function.apply(mockAtomicLong.get());
    System.out.println("apply结果: " + result);
    System.out.println("apply后的原值: " + mockAtomicLong.get());

    // 模拟alter - 修改值但不返回结果
    mockAtomicLong.set(1);
    mockAtomicLong.set(add2Function.apply(mockAtomicLong.get()));
    System.out.println("alter后的值: " + mockAtomicLong.get());

    // 模拟alterAndGet - 修改值并返回新值
    mockAtomicLong.set(1);
    long newValue = mockAtomicLong.updateAndGet(v -> add2Function.apply(v));
    System.out.println("alterAndGet结果: " + newValue);
    System.out.println("alterAndGet后的值: " + mockAtomicLong.get());

    // 模拟getAndAlter - 修改值并返回旧值
    mockAtomicLong.set(1);
    long oldValue = mockAtomicLong.getAndUpdate(v -> add2Function.apply(v));
    System.out.println("getAndAlter结果: " + oldValue);
    System.out.println("getAndAlter后的值: " + mockAtomicLong.get());
  }

  /**
   * 演示多线程环境下IAtomicLong的使用
   * 
   * <p>
   * 展示在高并发环境下，如何使用IAtomicLong安全地更新共享计数器而不引入竞态条件。
   * </p>
   * 
   * <p>
   * <strong>并发操作的关键点：</strong>
   * </p>
   * <ul>
   * <li><strong>线程安全：</strong> 即使多个线程/节点同时访问，也能保证操作的原子性</li>
   * <li><strong>一致性：</strong> 所有节点最终会看到相同的值</li>
   * <li><strong>无锁操作：</strong> 不需要显式锁即可实现线程安全的计数</li>
   * </ul>
   * 
   * <p>
   * 这个示例特别适合展示在分布式环境中多节点同时更新同一计数器的场景，
   * 如统计集群总请求数、限流或分布式ID生成等。
   * </p>
   */
  public void concurrentIncrementExample() {
    System.out.println("\n-- 多线程环境下的IAtomicLong --");

    // 重置模拟计数器
    mockAtomicLong.set(0);

    // 定义线程数量
    int threadCount = 5;
    // 每个线程递增次数
    int incrementsPerThread = 1000;

    System.out.println("创建 " + threadCount + " 个线程，每个执行 " + incrementsPerThread + " 次递增操作");

    // 创建并启动多个线程
    Thread[] threads = new Thread[threadCount];
    for (int i = 0; i < threadCount; i++) {
      threads[i] = new Thread(() -> {
        for (int j = 0; j < incrementsPerThread; j++) {
          mockAtomicLong.incrementAndGet();
        }
      });
      threads[i].start();
    }

    // 等待所有线程完成
    try {
      for (Thread thread : threads) {
        thread.join();
      }
    } catch (InterruptedException e) {
      System.err.println("线程等待被中断: " + e.getMessage());
    }

    System.out.println("预期最终值: " + (threadCount * incrementsPerThread));
    System.out.println("实际最终值: " + mockAtomicLong.get());
    System.out.println("两者是否相等: " + (mockAtomicLong.get() == threadCount * incrementsPerThread));
  }
}