package org.example.hazelcast.demo.events;

import org.springframework.stereotype.Component;

import java.util.Scanner;

/**
 * Hazelcast事件演示运行器
 * 提供用户界面以运行事件演示
 */
@Component
public class EventsDemoRunner {
  private final EventsDemo eventsDemo;
  private final Scanner scanner;

  public EventsDemoRunner(EventsDemo eventsDemo) {
    this.eventsDemo = eventsDemo;
    this.scanner = new Scanner(System.in);
  }

  /**
   * 运行事件演示
   */
  public void eventsRunner() {
    System.out.println("\n===== Hazelcast分布式事件演示 =====");
    System.out.println("这个演示将展示Hazelcast的事件监听机制");

    boolean exit = false;
    while (!exit) {
      showEventsMenu();
      int choice = getUserChoice();

      switch (choice) {
        case 1:
          eventsDemo.runAllDemos();
          break;
        case 2:
          eventsDemo.demoMapEntryListener();
          break;
        case 3:
          eventsDemo.demoCollectionItemListener();
          break;
        case 4:
          eventsDemo.demoTopicMessageListener();
          break;
        case 5:
          eventsDemo.demoLifecycleListener();
          break;
        case 6:
          eventsDemo.demoMembershipListener();
          break;
        case 7:
          eventsDemo.demoDistributedObjectListener();
          break;
        case 0:
          exit = true;
          System.out.println("返回主菜单...");
          break;
        default:
          System.out.println("无效选择，请重试");
      }

      if (!exit) {
        waitForKeyPress();
      }
    }
  }

  /**
   * 显示事件演示菜单
   */
  private void showEventsMenu() {
    System.out.println("\n----- 事件监听器演示菜单 -----");
    System.out.println("1. 运行所有事件演示");
    System.out.println("2. 演示Map条目监听器");
    System.out.println("3. 演示集合项目监听器");
    System.out.println("4. 演示Topic消息监听器");
    System.out.println("5. 演示生命周期监听器");
    System.out.println("6. 演示成员关系监听器");
    System.out.println("7. 演示分布式对象监听器");
    System.out.println("0. 返回主菜单");
    System.out.print("请选择: ");
  }

  /**
   * 获取用户输入的选择
   */
  private int getUserChoice() {
    try {
      return Integer.parseInt(scanner.nextLine().trim());
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