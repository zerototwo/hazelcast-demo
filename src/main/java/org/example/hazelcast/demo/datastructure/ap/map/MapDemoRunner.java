package org.example.hazelcast.demo.datastructure.ap.map;

import com.hazelcast.core.HazelcastInstance;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.Scanner;

/**
 * Map示例运行器
 * 提供Map数据结构各种操作的示例运行入口
 */
@Component
public class MapDemoRunner {

  private final MapBasicOperationsDemo basicOperationsDemo;
  private final MapQueryDemo queryDemo;
  private final MapAggregationDemo aggregationDemo;
  private final MapListenersDemo listenersDemo;
  private final MapEntryProcessorDemo entryProcessorDemo;
  private final MapLockingDemo lockingDemo;
  private final HazelcastInstance hazelcastInstance;

  public MapDemoRunner(
      MapBasicOperationsDemo basicOperationsDemo,
      MapQueryDemo queryDemo,
      MapAggregationDemo aggregationDemo,
      MapListenersDemo listenersDemo,
      MapEntryProcessorDemo entryProcessorDemo,
      MapLockingDemo lockingDemo,
      HazelcastInstance hazelcastInstance) {
    this.basicOperationsDemo = basicOperationsDemo;
    this.queryDemo = queryDemo;
    this.aggregationDemo = aggregationDemo;
    this.listenersDemo = listenersDemo;
    this.entryProcessorDemo = entryProcessorDemo;
    this.lockingDemo = lockingDemo;
    this.hazelcastInstance = hazelcastInstance;
  }

  /**
   * 运行Map示例
   */
  public void mapRunner() {
    try {
      while (true) {
        printMapMenu();
        int choice = getUserChoice();

        switch (choice) {
          case 0:
            System.out.println("返回主菜单...");
            return;
          case 1:
            basicOperationsDemo.runAllExamples();
            break;
          case 2:
            queryDemo.runAllExamples();
            break;
          case 3:
            aggregationDemo.runAllExamples();
            break;
          case 4:
            listenersDemo.runAllExamples();
            break;
          case 5:
            entryProcessorDemo.runAllExamples();
            break;
          case 6:
            lockingDemo.runAllExamples();
            break;
          case 7:
            // 运行所有示例
            basicOperationsDemo.runAllExamples();
            waitForKeyPress();

            queryDemo.runAllExamples();
            waitForKeyPress();

            aggregationDemo.runAllExamples();
            waitForKeyPress();

            listenersDemo.runAllExamples();
            waitForKeyPress();

            entryProcessorDemo.runAllExamples();
            waitForKeyPress();

            lockingDemo.runAllExamples();
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
      System.err.println("运行Map示例时出错: " + e.getMessage());
      e.printStackTrace();
    }
  }

  /**
   * 打印Map菜单
   */
  private void printMapMenu() {
    System.out.println("\n请选择要运行的Map示例：");
    System.out.println("1. 基本CRUD操作示例");
    System.out.println("2. 查询和索引示例");
    System.out.println("3. 聚合操作示例");
    System.out.println("4. 监听器和事件处理示例");
    System.out.println("5. 入口处理器示例");
    System.out.println("6. 锁定和同步示例");
    System.out.println("7. 运行所有示例");
    System.out.println("0. 返回上级菜单");
    System.out.print("请输入选择 [0-7]: ");
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