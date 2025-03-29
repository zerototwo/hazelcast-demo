package org.example.hazelcast.demo.datastructure.cp.cpmap;

import com.hazelcast.core.HazelcastInstance;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.Scanner;

/**
 * CPMap示例运行器
 * 提供CPMap数据结构各种操作的示例运行入口
 * 注意: CPMap功能仅在Hazelcast企业版中可用
 */
@Component
public class CPMapDemoRunner {

  private final CPMapBasicOperationsDemo cpMapBasicOperationsDemo;
  private final HazelcastInstance hazelcastInstance;

  public CPMapDemoRunner(CPMapBasicOperationsDemo cpMapBasicOperationsDemo,
      HazelcastInstance hazelcastInstance) {
    this.cpMapBasicOperationsDemo = cpMapBasicOperationsDemo;
    this.hazelcastInstance = hazelcastInstance;
  }

  /**
   * 运行CPMap示例
   */
  public void cpMapRunner() {
    System.out.println("\n注意: CPMap功能仅在Hazelcast企业版中可用。");
    System.out.println("如果您使用的是社区版，这些示例将无法正常运行，但代码仍可作为参考。\n");

    try {
      while (true) {
        showCPMapMenu();
        int choice = getUserChoice();

        switch (choice) {
          case 0:
            System.out.println("返回主菜单...");
            return;
          case 1:
            cpMapBasicOperationsDemo.runAllExamples();
            break;
          case 2:
            cpMapBasicOperationsDemo.basicOperationsExample();
            break;
          case 3:
            cpMapBasicOperationsDemo.atomicOperationsExample();
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
      System.err.println("运行CPMap示例时出错: " + e.getMessage());
      e.printStackTrace();
    }
  }

  /**
   * 显示CPMap菜单
   */
  private void showCPMapMenu() {
    System.out.println("\n请选择要运行的CPMap示例：");
    System.out.println("1. 运行所有CPMap示例");
    System.out.println("2. 基本操作示例");
    System.out.println("3. 原子操作示例");
    System.out.println("0. 返回上级菜单");
    System.out.print("请输入选择 [0-3]: ");
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