package org.example.hazelcast.demo.eventjournal.eventjournal;

import com.hazelcast.core.HazelcastInstance;
import org.springframework.stereotype.Component;

import java.util.Scanner;

/**
 * Event Journal示例运行器
 * <p>
 * 此类提供了一个交互式菜单界面，用于运行不同的Event Journal操作示例。
 * 用户可以选择运行所有示例，或单独运行特定的示例来了解Event Journal的不同功能。
 * </p>
 */
@Component
public class EventJournalDemoRunner {

  private final EventJournalBasicOperationsDemo eventJournalBasicOperationsDemo;
  private final HazelcastInstance hazelcastInstance;
  private final Scanner scanner;

  /**
   * 构造函数，注入所需的依赖
   * 
   * @param eventJournalBasicOperationsDemo Event Journal基本操作示例对象
   * @param hazelcastInstance               Hazelcast实例
   */
  public EventJournalDemoRunner(EventJournalBasicOperationsDemo eventJournalBasicOperationsDemo,
      HazelcastInstance hazelcastInstance) {
    this.eventJournalBasicOperationsDemo = eventJournalBasicOperationsDemo;
    this.hazelcastInstance = hazelcastInstance;
    this.scanner = new Scanner(System.in);
  }

  /**
   * Event Journal示例运行入口
   * <p>
   * 显示菜单并处理用户选择，运行相应的Event Journal示例
   * </p>
   */
  public void eventJournalRunner() {
    boolean exit = false;

    while (!exit) {
      showEventJournalMenu();
      int choice = getUserChoice();

      switch (choice) {
        case 1:
          eventJournalBasicOperationsDemo.runAllExamples();
          break;
        case 2:
          eventJournalBasicOperationsDemo.setupEventJournalMap();
          break;
        case 3:
          eventJournalBasicOperationsDemo.basicReadFromEventJournal();
          break;
        case 4:
          eventJournalBasicOperationsDemo.filteringEventsExample();
          break;
        case 5:
          eventJournalBasicOperationsDemo.projectionExample();
          break;
        case 6:
          eventJournalBasicOperationsDemo.readingFromSequenceExample();
          break;
        case 7:
          eventJournalBasicOperationsDemo.liveEventMonitoringExample();
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
   * 显示Event Journal示例菜单
   */
  private void showEventJournalMenu() {
    System.out.println("\n===== Event Journal示例菜单 =====");
    System.out.println("1. 运行所有Event Journal示例");
    System.out.println("2. 设置Event Journal Map");
    System.out.println("3. 基本读取示例");
    System.out.println("4. 事件过滤示例");
    System.out.println("5. 事件投影示例");
    System.out.println("6. 从特定序列号读取示例");
    System.out.println("7. 实时事件监控示例");
    System.out.println("0. 返回主菜单");
    System.out.print("请选择 (0-7): ");
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