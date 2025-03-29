package org.example.hazelcast.demo.cp.isemaphore;

import com.hazelcast.core.HazelcastInstance;
import org.springframework.stereotype.Component;

import java.util.Scanner;

/**
 * ISemaphore示例运行器
 * <p>
 * 此类提供了一个交互式菜单界面，用于运行不同的ISemaphore操作示例。
 * 用户可以选择运行所有示例，或单独运行特定的示例来了解ISemaphore的不同功能。
 * </p>
 */
@Component
public class ISemaphoreDemoRunner {

  private final ISemaphoreBasicOperationsDemo isemaphoreBasicOperationsDemo;
  private final HazelcastInstance hazelcastInstance;
  private final Scanner scanner;

  /**
   * 构造函数，注入所需的依赖
   * 
   * @param isemaphoreBasicOperationsDemo ISemaphore基本操作示例对象
   * @param hazelcastInstance             Hazelcast实例
   */
  public ISemaphoreDemoRunner(ISemaphoreBasicOperationsDemo isemaphoreBasicOperationsDemo,
      HazelcastInstance hazelcastInstance) {
    this.isemaphoreBasicOperationsDemo = isemaphoreBasicOperationsDemo;
    this.hazelcastInstance = hazelcastInstance;
    this.scanner = new Scanner(System.in);
  }

  /**
   * ISemaphore示例运行入口
   * <p>
   * 显示菜单并处理用户选择，运行相应的ISemaphore示例
   * </p>
   */
  public void isemaphoreRunner() {
    boolean exit = false;

    while (!exit) {
      showISemaphoreMenu();
      int choice = getUserChoice();

      switch (choice) {
        case 1:
          isemaphoreBasicOperationsDemo.runAllExamples();
          break;
        case 2:
          isemaphoreBasicOperationsDemo.basicOperationsExample();
          break;
        case 3:
          isemaphoreBasicOperationsDemo.resourceLimitExample();
          break;
        case 4:
          isemaphoreBasicOperationsDemo.multiPermitsExample();
          break;
        case 5:
          isemaphoreBasicOperationsDemo.timeoutExample();
          break;
        case 6:
          isemaphoreBasicOperationsDemo.fairnessExample();
          break;
        case 0:
          exit = true;
          break;
        default:
          System.out.println("无效选择，请重新输入");
      }

      if (!exit) {
        waitForKeyPress();
      }
    }
  }

  /**
   * 显示ISemaphore示例菜单
   */
  private void showISemaphoreMenu() {
    System.out.println("\n===== ISemaphore示例菜单 =====");
    System.out.println("1. 运行所有ISemaphore示例");
    System.out.println("2. 基本操作示例");
    System.out.println("3. 资源限制示例");
    System.out.println("4. 多许可证获取和释放示例");
    System.out.println("5. 带超时的许可证获取示例");
    System.out.println("6. 信号量公平性示例");
    System.out.println("0. 返回主菜单");
    System.out.print("请选择 (0-6): ");
  }

  /**
   * 获取用户输入的选择
   * 
   * @return 用户的选择
   */
  private int getUserChoice() {
    try {
      return Integer.parseInt(scanner.nextLine());
    } catch (NumberFormatException e) {
      return -1;
    }
  }

  /**
   * 等待用户按键继续
   */
  private void waitForKeyPress() {
    System.out.println("\n按回车键继续...");
    scanner.nextLine();
  }
}