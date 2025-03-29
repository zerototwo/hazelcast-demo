package org.example.hazelcast.demo.cp.fencedlock;

import com.hazelcast.core.HazelcastInstance;
// 注意: 以下导入在社区版中不可用，仅在企业版中可用
// import com.hazelcast.cp.lock.FencedLock;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * FencedLock基本操作示例
 * FencedLock是java.util.concurrent.locks.Lock的线性一致性分布式实现，
 * 确保整个集群中只有一个线程能够执行被锁保护的临界区。
 * 
 * 注意: FencedLock功能仅在Hazelcast企业版中可用。
 * 本示例代码使用模拟实现，仅用于演示API用法，无法在社区版中实际运行。
 */
@Component
public class FencedLockBasicOperationsDemo {

  private final HazelcastInstance hazelcastInstance;
  // 用于模拟fencing token的计数器
  private final AtomicLong fencingTokenGenerator = new AtomicLong(0);

  public FencedLockBasicOperationsDemo(HazelcastInstance hazelcastInstance) {
    this.hazelcastInstance = hazelcastInstance;
  }

  /**
   * 运行所有FencedLock示例
   */
  public void runAllExamples() {
    System.out.println("\n==== FencedLock基本操作示例 (仅企业版支持) ====");
    System.out.println("注意: 以下示例仅在Hazelcast企业版中可用，当前为模拟演示");
    basicLockUnlockExample();
    tryLockWithTimeoutExample();
    fencingTokenExample();
    System.out.println("==== FencedLock示例结束 ====\n");
  }

  /**
   * 演示FencedLock的基本lock/unlock操作
   */
  public void basicLockUnlockExample() {
    System.out.println("\n-- FencedLock基本lock/unlock操作 --");
    System.out.println("模拟FencedLock操作，实际需要企业版支持");

    // 模拟操作，实际在企业版中会使用:
    // FencedLock lock = hazelcastInstance.getCPSubsystem().getLock("myLock");

    // 使用ReentrantLock模拟FencedLock
    ReentrantLock lock = new ReentrantLock();

    try {
      // 获取锁
      System.out.println("尝试获取锁...");
      lock.lock();
      // 实际在企业版中会返回fencing token:
      // long fencingToken = lock.lock();
      long mockFencingToken = fencingTokenGenerator.incrementAndGet();
      System.out.println("锁获取成功，栅栏令牌: " + mockFencingToken);

      System.out.println("执行临界区代码...");

      // 模拟在临界区执行一些操作
      Thread.sleep(1000);

    } catch (Exception e) {
      System.err.println("锁操作期间出错: " + e.getMessage());
    } finally {
      // 释放锁
      if (lock.isHeldByCurrentThread()) {
        lock.unlock();
        System.out.println("锁已释放");
      }
    }
  }

  /**
   * 演示FencedLock的tryLock带超时操作
   */
  public void tryLockWithTimeoutExample() {
    System.out.println("\n-- FencedLock的tryLock带超时操作 --");
    System.out.println("模拟FencedLock操作，实际需要企业版支持");

    // 模拟操作，实际在企业版中会使用:
    // FencedLock lock = hazelcastInstance.getCPSubsystem().getLock("myLock");

    // 使用ReentrantLock模拟FencedLock
    ReentrantLock lock = new ReentrantLock();

    try {
      // 尝试获取锁，设置超时时间为5秒
      System.out.println("尝试获取锁，最多等待5秒...");
      boolean locked = lock.tryLock(5, TimeUnit.SECONDS);

      // 实际在企业版中会返回fencing token:
      // long fencingToken = lock.tryLock(5, TimeUnit.SECONDS);
      // if (fencingToken != 0) { ... }

      if (locked) {
        long mockFencingToken = fencingTokenGenerator.incrementAndGet();
        System.out.println("锁获取成功，栅栏令牌: " + mockFencingToken);

        try {
          System.out.println("执行临界区代码...");

          // 模拟在临界区执行一些操作
          Thread.sleep(1000);

        } finally {
          // 释放锁
          lock.unlock();
          System.out.println("锁已释放");
        }
      } else {
        System.out.println("无法在指定时间内获取锁");
      }
    } catch (Exception e) {
      System.err.println("锁操作期间出错: " + e.getMessage());
    }
  }

  /**
   * 演示FencedLock的栅栏令牌特性
   * 这是FencedLock的关键特性，可以防止"脑裂"问题
   */
  public void fencingTokenExample() {
    System.out.println("\n-- FencedLock的栅栏令牌特性 --");
    System.out.println("模拟FencedLock操作，实际需要企业版支持");

    // 模拟操作，实际在企业版中会使用:
    // FencedLock lock =
    // hazelcastInstance.getCPSubsystem().getLock("myResourceLock");

    // 使用ReentrantLock模拟FencedLock
    ReentrantLock lock = new ReentrantLock();

    try {
      System.out.println("模拟两个客户端使用FencedLock访问共享资源的场景");
      System.out.println("这展示了栅栏令牌如何防止由GC暂停或网络问题引起的并发访问问题\n");

      // 客户端1获取锁
      System.out.println("客户端1: 尝试获取锁...");
      lock.lock();
      // 实际企业版中: long fencingToken1 = lock.lock();
      long mockFencingToken1 = fencingTokenGenerator.incrementAndGet();
      System.out.println("客户端1: 锁获取成功，栅栏令牌: " + mockFencingToken1);

      // 客户端1向外部服务发送请求，包含其栅栏令牌
      System.out.println("客户端1: 向外部服务发送请求，包含栅栏令牌: " + mockFencingToken1);

      // 模拟客户端1突然进入GC暂停，导致其会话超时
      System.out.println("客户端1: 进入长时间GC暂停...");

      // 这里实际上违背了锁的语义，但我们是为了模拟会话超时和锁的自动释放
      lock.unlock();

      // 客户端2获取同一个锁
      System.out.println("客户端2: 尝试获取锁...");
      lock.lock();
      // 实际企业版中: long fencingToken2 = lock.lock();
      long mockFencingToken2 = fencingTokenGenerator.incrementAndGet();
      System.out.println("客户端2: 锁获取成功，栅栏令牌: " + mockFencingToken2);

      // 客户端2向外部服务发送请求，包含其栅栏令牌
      System.out.println("客户端2: 向外部服务发送请求，包含栅栏令牌: " + mockFencingToken2);

      // 模拟客户端1从GC暂停中恢复，并尝试访问外部服务
      System.out.println("客户端1: 从GC暂停中恢复，继续其请求...");

      // 模拟外部服务检查栅栏令牌
      System.out.println("外部服务: 收到来自客户端1的请求，栅栏令牌: " + mockFencingToken1);
      if (mockFencingToken1 < mockFencingToken2) {
        System.out.println("外部服务: 拒绝客户端1的请求，因为其栅栏令牌已过时");
      } else {
        System.out.println("外部服务: 接受客户端1的请求");
      }

      System.out.println("外部服务: 收到来自客户端2的请求，栅栏令牌: " + mockFencingToken2);
      if (mockFencingToken2 >= mockFencingToken1) {
        System.out.println("外部服务: 接受客户端2的请求，因为其栅栏令牌是最新的");
      } else {
        System.out.println("外部服务: 拒绝客户端2的请求");
      }

      // 释放锁
      if (lock.isHeldByCurrentThread()) {
        lock.unlock();
        System.out.println("客户端2: 锁已释放");
      }

    } catch (Exception e) {
      System.err.println("锁操作期间出错: " + e.getMessage());
    }

    // 解释栅栏令牌的重要性
    System.out.println("\n栅栏令牌的重要性:");
    System.out.println("1. 每次锁被分配给新的所有者时，栅栏令牌都会递增");
    System.out.println("2. 栅栏令牌可以传递给外部服务，确保按顺序执行锁持有者执行的操作");
    System.out.println("3. 这可以防止由于网络分区、GC暂停等引起的并发访问问题");
    System.out.println("4. 即使客户端认为自己仍持有锁，外部服务也可以通过栅栏令牌拒绝过时的请求");
  }

  /**
   * 演示正确使用FencedLock的模式：使用try-finally块
   */
  public void lockWithTryFinallyExample() {
    System.out.println("\n-- 使用try-finally块的FencedLock模式 --");
    System.out.println("模拟FencedLock操作，实际需要企业版支持");

    // 模拟操作，实际在企业版中会使用:
    // FencedLock lock = hazelcastInstance.getCPSubsystem().getLock("myLock");

    // 使用ReentrantLock模拟FencedLock
    ReentrantLock lock = new ReentrantLock();

    // 获取锁操作在try块外部，因为我们不希望在锁获取失败时解锁
    lock.lock();
    // 实际在企业版中会返回fencing token: long fencingToken = lock.lock();
    long mockFencingToken = fencingTokenGenerator.incrementAndGet();
    System.out.println("锁获取成功，栅栏令牌: " + mockFencingToken);

    try {
      // 在此执行受锁保护的操作
      System.out.println("执行临界区代码...");

      // 模拟在临界区执行一些操作
      Thread.sleep(500);

    } catch (Exception e) {
      System.err.println("临界区操作出错: " + e.getMessage());
    } finally {
      // 在finally块中释放锁，确保即使出现异常也能释放锁
      lock.unlock();
      System.out.println("锁已释放");
    }
  }
}