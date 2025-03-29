package org.example.hazelcast.demo.datastructure.cp.fencedlock;

import com.hazelcast.core.HazelcastInstance;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.Scanner;

/**
 * FencedLock示例运行器
 * 提供FencedLock数据结构各种操作的示例运行入口
 * 注意: FencedLock功能仅在Hazelcast企业版中可用
 */
@Component
public class FencedLockDemoRunner {

  private final FencedLockBasicOperationsDemo fencedLockBasicOperationsDemo;
  private final HazelcastInstance hazelcastInstance;

  public FencedLockDemoRunner(FencedLockBasicOperationsDemo fencedLockBasicOperationsDemo,
      HazelcastInstance hazelcastInstance) {
    this.fencedLockBasicOperationsDemo = fencedLockBasicOperationsDemo;
    this.hazelcastInstance = hazelcastInstance;
  }

  /**
   * 运行FencedLock示例
   */
  public void fencedLockRunner() {
    System.out.println("\n注意: FencedLock功能仅在Hazelcast企业版中可用。");
    System.out.println("如果您使用的是社区版，这些示例将无法正常运行，但代码仍可作为参考。\n");

    try {
      while (true) {
        showFencedLockMenu();
        int choice = getUserChoice();

        switch (choice) {
          case 0:
            System.out.println("返回主菜单...");
            return;
          case 1:
            fencedLockBasicOperationsDemo.runAllExamples();
            break;
          case 2:
            fencedLockBasicOperationsDemo.basicLockUnlockExample();
            break;
          case 3:
            fencedLockBasicOperationsDemo.tryLockWithTimeoutExample();
            break;
          case 4:
            fencedLockBasicOperationsDemo.fencingTokenExample();
            break;
          case 5:
            fencedLockBasicOperationsDemo.lockWithTryFinallyExample();
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
      System.err.println("运行FencedLock示例时出错: " + e.getMessage());
      e.printStackTrace();
    }
  }

  /**
   * 显示FencedLock菜单
   */
  private void showFencedLockMenu() {
    System.out.println("\n请选择要运行的FencedLock示例：");
    System.out.println("1. 运行所有FencedLock示例");
    System.out.println("2. 基本lock/unlock操作示例");
    System.out.println("3. tryLock带超时操作示例");
    System.out.println("4. 栅栏令牌特性示例");
    System.out.println("5. 正确使用try-finally块的模式");
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