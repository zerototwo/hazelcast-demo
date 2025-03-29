package org.example.hazelcast.demo.compute.executor;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IExecutorService;
import com.hazelcast.cluster.Member;
import org.springframework.stereotype.Component;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * 演示Hazelcast Executor Service的使用方法。
 * 
 * Executor Service允许您在Hazelcast集群的成员上分布式执行任务，
 * 提供类似于Java标准ExecutorService的API，但具有分布式特性。
 * 
 * 主要特点：
 * - 任务分发：在集群中将任务分发到特定或所有成员
 * - 结果处理：同步或异步获取任务结果
 * - 路由策略：支持多种任务路由方式（随机、轮询、特定成员等）
 * - 负载均衡：在集群中分散工作负载
 */
@Component
public class ExecutorServiceDemo {

  private final HazelcastInstance hazelcastInstance;

  public ExecutorServiceDemo(HazelcastInstance hazelcastInstance) {
    this.hazelcastInstance = hazelcastInstance;
  }

  /**
   * 运行所有ExecutorService示例
   */
  public void runAllExamples() {
    System.out.println("===== Running All ExecutorService Examples =====");

    basicExecutorExample();
    System.out.println();

    executeOnAllMembersExample();
    System.out.println();

    executeOnKeyOwnerExample();
    System.out.println();

    executeWithCallbackExample();
    System.out.println("===============================================");
  }

  /**
   * 基本执行器示例
   * 展示如何提交简单任务到执行器服务
   */
  public void basicExecutorExample() {
    System.out.println("--- Basic Executor Example ---");

    // 获取或创建ExecutorService
    IExecutorService executorService = hazelcastInstance.getExecutorService("demo-executor");

    // 创建任务
    SimpleTask task = new SimpleTask("基本执行任务");

    try {
      // 提交任务
      Future<String> future = executorService.submit(task);

      // 等待并获取结果
      String result = future.get();
      System.out.println("任务结果: " + result);

    } catch (InterruptedException | ExecutionException e) {
      System.err.println("任务执行失败: " + e.getMessage());
    }
  }

  /**
   * 在所有成员上执行的示例
   * 展示如何在所有集群成员上执行任务
   */
  public void executeOnAllMembersExample() {
    System.out.println("--- Execute on All Members Example ---");

    // 获取ExecutorService
    IExecutorService executorService = hazelcastInstance.getExecutorService("demo-executor");

    // 创建任务
    MemberInfoTask infoTask = new MemberInfoTask();

    try {
      // 提交到所有成员
      Map<Member, Future<String>> futureMap = executorService.submitToAllMembers(infoTask);

      // 收集结果
      for (Map.Entry<Member, Future<String>> entry : futureMap.entrySet()) {
        Member member = entry.getKey();
        String result = entry.getValue().get();
        System.out.println("成员 " + member.getAddress() + " 信息: " + result);
      }
    } catch (InterruptedException | ExecutionException e) {
      System.err.println("在所有成员上执行任务失败: " + e.getMessage());
    }
  }

  /**
   * 在键所有者上执行的示例
   * 展示如何在拥有特定键的成员上执行任务
   */
  public void executeOnKeyOwnerExample() {
    System.out.println("--- Execute on Key Owner Example ---");

    // 获取ExecutorService
    IExecutorService executorService = hazelcastInstance.getExecutorService("demo-executor");

    // 定义键和相关任务
    String key = "product-1001";
    KeyAwareTask keyTask = new KeyAwareTask(key);

    try {
      // 提交到键的所有者
      Future<String> future = executorService.submitToKeyOwner(keyTask, key);

      // 获取结果
      String result = future.get();
      System.out.println("键 '" + key + "' 的处理结果: " + result);

    } catch (InterruptedException | ExecutionException e) {
      System.err.println("在键所有者上执行任务失败: " + e.getMessage());
    }
  }

  /**
   * 使用回调的示例
   * 展示如何使用异步回调处理执行结果
   */
  public void executeWithCallbackExample() {
    System.out.println("--- Execute with Callback Example ---");

    // 获取ExecutorService
    IExecutorService executorService = hazelcastInstance.getExecutorService("demo-executor");

    // 创建多个任务
    Map<String, DelayedTask> tasks = new HashMap<>();
    tasks.put("quick-task", new DelayedTask(1));
    tasks.put("medium-task", new DelayedTask(2));
    tasks.put("slow-task", new DelayedTask(3));

    // 记录开始时间
    long startTime = System.currentTimeMillis();
    System.out.println("开始提交任务...");

    // 提交所有任务
    Map<String, Future<String>> futures = new HashMap<>();
    for (Map.Entry<String, DelayedTask> entry : tasks.entrySet()) {
      String taskName = entry.getKey();
      DelayedTask task = entry.getValue();
      futures.put(taskName, executorService.submit(task));
    }

    // 异步处理结果
    for (Map.Entry<String, Future<String>> entry : futures.entrySet()) {
      String taskName = entry.getKey();
      Future<String> future = entry.getValue();

      try {
        // 非阻塞处理结果
        if (future.isDone()) {
          System.out.println(taskName + " 结果立即获取: " + future.get());
        } else {
          System.out.println(taskName + " 仍在执行中...");
        }
      } catch (InterruptedException | ExecutionException e) {
        System.err.println(taskName + " 执行失败: " + e.getMessage());
      }
    }

    // 等待所有任务完成
    try {
      System.out.println("等待所有任务完成...");
      for (Map.Entry<String, Future<String>> entry : futures.entrySet()) {
        String taskName = entry.getKey();
        Future<String> future = entry.getValue();
        String result = future.get(); // 这里会阻塞等待
        long elapsedTime = System.currentTimeMillis() - startTime;
        System.out.println(taskName + " 完成，耗时 " + elapsedTime + " ms，结果: " + result);
      }
    } catch (InterruptedException | ExecutionException e) {
      System.err.println("等待任务完成时出错: " + e.getMessage());
    }
  }

  /**
   * 简单任务示例类
   * 实现Callable接口，执行简单计算并返回结果
   */
  private static class SimpleTask implements Callable<String>, Serializable {
    private final String name;

    public SimpleTask(String name) {
      this.name = name;
    }

    @Override
    public String call() throws Exception {
      Thread.sleep(500); // 模拟工作负载
      return name + " 在 " + Thread.currentThread().getName() + " 上执行完成";
    }
  }

  /**
   * 成员信息任务类
   * 返回运行此任务的成员的信息
   */
  private static class MemberInfoTask implements Callable<String>, Serializable {
    @Override
    public String call() throws Exception {
      // 获取当前线程和运行时信息
      String threadName = Thread.currentThread().getName();
      long freeMemory = Runtime.getRuntime().freeMemory() / (1024 * 1024);
      int cpuCores = Runtime.getRuntime().availableProcessors();

      return String.format("线程: %s, 可用内存: %d MB, CPU核心: %d",
          threadName, freeMemory, cpuCores);
    }
  }

  /**
   * 键感知任务类
   * 处理特定键相关的操作
   */
  private static class KeyAwareTask implements Callable<String>, Serializable {
    private final String key;

    public KeyAwareTask(String key) {
      this.key = key;
    }

    @Override
    public String call() throws Exception {
      // 模拟针对特定键的处理
      Thread.sleep(300);
      String threadName = Thread.currentThread().getName();
      return String.format("键 '%s' 在 %s 上处理完成", key, threadName);
    }
  }

  /**
   * 延迟任务类
   * 模拟需要不同时间来完成的任务
   */
  private static class DelayedTask implements Callable<String>, Serializable {
    private final int delaySeconds;

    public DelayedTask(int delaySeconds) {
      this.delaySeconds = delaySeconds;
    }

    @Override
    public String call() throws Exception {
      // 模拟工作负载
      Thread.sleep(delaySeconds * 1000);
      return "延迟 " + delaySeconds + " 秒的任务在 " +
          Thread.currentThread().getName() + " 上完成";
    }
  }
}