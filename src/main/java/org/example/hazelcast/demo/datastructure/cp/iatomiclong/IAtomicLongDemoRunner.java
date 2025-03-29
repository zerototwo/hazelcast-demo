package org.example.hazelcast.demo.datastructure.cp.iatomiclong;

import com.hazelcast.core.HazelcastInstance;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.Scanner;

/**
 * IAtomicLong示例运行器
 * 提供IAtomicLong数据结构各种操作的示例运行入口
 */
@Component
public class IAtomicLongDemoRunner {

  private final IAtomicLongBasicOperationsDemo iAtomicLongBasicOperationsDemo;
  private final HazelcastInstance hazelcastInstance;

  public IAtomicLongDemoRunner(IAtomicLongBasicOperationsDemo iAtomicLongBasicOperationsDemo,
      HazelcastInstance hazelcastInstance) {
    this.iAtomicLongBasicOperationsDemo = iAtomicLongBasicOperationsDemo;
    this.hazelcastInstance = hazelcastInstance;
  }

  /**
   * 运行IAtomicLong示例
   */
  public void iAtomicLongRunner() {
    System.out.println("\nIAtomicLong是Hazelcast的CP数据结构，提供分布式环境下的原子长整型操作。");
    System.out.println("完整功能在Hazelcast企业版中可用，社区版中有基本支持。\n");

    try {
      while (true) {
        showIAtomicLongMenu();
        int choice = getUserChoice();

        switch (choice) {
          case 0:
            System.out.println("返回主菜单...");
            return;
          case 1:
            iAtomicLongBasicOperationsDemo.runAllExamples();
            break;
          case 2:
            iAtomicLongBasicOperationsDemo.basicOperationsExample();
            break;
          case 3:
            iAtomicLongBasicOperationsDemo.incrementCounterExample();
            break;
          case 4:
            iAtomicLongBasicOperationsDemo.applyFunctionsExample();
            break;
          case 5:
            iAtomicLongBasicOperationsDemo.concurrentIncrementExample();
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
      System.err.println("运行IAtomicLong示例时出错: " + e.getMessage());
      e.printStackTrace();
    }
  }

  /**
   * 显示IAtomicLong菜单
   */
  private void showIAtomicLongMenu() {
    System.out.println("\n请选择要运行的IAtomicLong示例：");
    System.out.println("1. 运行所有IAtomicLong示例");
    System.out.println("2. 基本操作示例");
    System.out.println("3. 分布式计数器示例");
    System.out.println("4. 函数应用示例");
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