package org.example.hazelcast.demo.cp.iatomicreference;

import com.hazelcast.core.HazelcastInstance;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.Scanner;

/**
 * IAtomicReference示例运行器
 * 提供IAtomicReference数据结构各种操作的示例运行入口
 */
@Component
public class IAtomicReferenceDemoRunner {

  private final IAtomicReferenceBasicOperationsDemo iAtomicReferenceBasicOperationsDemo;
  private final HazelcastInstance hazelcastInstance;

  public IAtomicReferenceDemoRunner(IAtomicReferenceBasicOperationsDemo iAtomicReferenceBasicOperationsDemo,
      HazelcastInstance hazelcastInstance) {
    this.iAtomicReferenceBasicOperationsDemo = iAtomicReferenceBasicOperationsDemo;
    this.hazelcastInstance = hazelcastInstance;
  }

  /**
   * 运行IAtomicReference示例
   */
  public void iAtomicReferenceRunner() {
    System.out.println("\nIAtomicReference是Hazelcast的CP数据结构，提供分布式环境下的引用对象原子操作。");
    System.out.println("完整功能在Hazelcast企业版中可用，社区版中有基本支持。\n");

    try {
      while (true) {
        showIAtomicReferenceMenu();
        int choice = getUserChoice();

        switch (choice) {
          case 0:
            System.out.println("返回主菜单...");
            return;
          case 1:
            iAtomicReferenceBasicOperationsDemo.runAllExamples();
            break;
          case 2:
            iAtomicReferenceBasicOperationsDemo.basicOperationsExample();
            break;
          case 3:
            iAtomicReferenceBasicOperationsDemo.functionApplicationExample();
            break;
          case 4:
            iAtomicReferenceBasicOperationsDemo.workingWithComplexObjectsExample();
            break;
          case 5:
            iAtomicReferenceBasicOperationsDemo.concurrentAccessExample();
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
      System.err.println("运行IAtomicReference示例时出错: " + e.getMessage());
      e.printStackTrace();
    }
  }

  /**
   * 显示IAtomicReference菜单
   */
  private void showIAtomicReferenceMenu() {
    System.out.println("\n请选择要运行的IAtomicReference示例：");
    System.out.println("1. 运行所有IAtomicReference示例");
    System.out.println("2. 基本操作示例");
    System.out.println("3. 函数应用示例");
    System.out.println("4. 复杂对象处理示例");
    System.out.println("5. 多线程并发示例");
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