package org.example.hazelcast.demo.datastructure.cp.icountdownlatch;

import com.hazelcast.core.HazelcastInstance;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.Scanner;

/**
 * ICountDownLatch示例运行器
 * 提供ICountDownLatch数据结构各种操作的示例运行入口
 */
@Component
public class ICountDownLatchDemoRunner {

  private final ICountDownLatchBasicOperationsDemo iCountDownLatchBasicOperationsDemo;
  private final HazelcastInstance hazelcastInstance;

  public ICountDownLatchDemoRunner(ICountDownLatchBasicOperationsDemo iCountDownLatchBasicOperationsDemo,
      HazelcastInstance hazelcastInstance) {
    this.iCountDownLatchBasicOperationsDemo = iCountDownLatchBasicOperationsDemo;
    this.hazelcastInstance = hazelcastInstance;
  }

  /**
   * 运行ICountDownLatch示例
   */
  public void iCountDownLatchRunner() {
    System.out.println("\nICountDownLatch是Hazelcast的CP数据结构，提供分布式环境下的计数同步机制。");
    System.out.println("与Java的CountDownLatch不同，计数完成后可以重置，但不能在活动计数期间重置。");
    System.out.println("完整功能在Hazelcast企业版中可用，社区版中有基本支持。\n");

    try {
      while (true) {
        showICountDownLatchMenu();
        int choice = getUserChoice();

        switch (choice) {
          case 0:
            System.out.println("返回主菜单...");
            return;
          case 1:
            iCountDownLatchBasicOperationsDemo.runAllExamples();
            break;
          case 2:
            iCountDownLatchBasicOperationsDemo.basicOperationsExample();
            break;
          case 3:
            iCountDownLatchBasicOperationsDemo.leaderFollowerExample();
            break;
          case 4:
            iCountDownLatchBasicOperationsDemo.parallelTasksExample();
            break;
          case 5:
            iCountDownLatchBasicOperationsDemo.trySetCountExample();
            break;
          default:
            System.out.println("无效选择，请重试。");
        }

        if (choice != 0) {
          System.out.println("\n示例执行完成。按回车键继续...");
          waitForKeyPress();
        }
      }
    } catch (Exception e) {
      System.err.println("运行ICountDownLatch示例时出错: " + e.getMessage());
      e.printStackTrace();
    }
  }

  /**
   * 显示ICountDownLatch菜单
   */
  private void showICountDownLatchMenu() {
    System.out.println("\n请选择要运行的ICountDownLatch示例：");
    System.out.println("1. 运行所有ICountDownLatch示例");
    System.out.println("2. 基本操作示例");
    System.out.println("3. 领导者-追随者模式示例");
    System.out.println("4. 并行任务协调示例");
    System.out.println("5. TrySetCount操作示例");
    System.out.println("0. 返回上级菜单");
    System.out.print("请输入选择 [0-5]: ");
  }

  /**
   * 获取用户输入
   */
  private int getUserChoice() {
    Scanner scanner = new Scanner(System.in);
    try {
      return scanner.nextInt();
    } catch (Exception e) {
      return -1;
    }
  }

  /**
   * 等待用户按键
   */
  private void waitForKeyPress() {
    try {
      System.in.read();
      // 清除输入缓冲
      while (System.in.available() > 0) {
        System.in.read();
      }
    } catch (IOException e) {
      // 忽略异常
    }
  }
}