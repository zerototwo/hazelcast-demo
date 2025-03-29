package org.example.hazelcast.demo.serializing;

import org.springframework.stereotype.Component;

import java.util.Scanner;

/**
 * Hazelcast序列化演示运行器
 * 提供用户界面以运行序列化演示
 */
@Component
public class SerializationDemoRunner {
  private final SerializationDemo serializationDemo;
  private final Scanner scanner;

  public SerializationDemoRunner(SerializationDemo serializationDemo) {
    this.serializationDemo = serializationDemo;
    this.scanner = new Scanner(System.in);
  }

  /**
   * 运行序列化演示
   */
  public void serializationRunner() {
    System.out.println("\n===== Hazelcast序列化演示 =====");
    System.out.println("这个演示将展示Hazelcast支持的不同序列化选项");

    boolean exit = false;
    while (!exit) {
      showSerializationMenu();
      int choice = getUserChoice();

      switch (choice) {
        case 1:
          serializationDemo.runDemo();
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
   * 显示序列化演示菜单
   */
  private void showSerializationMenu() {
    System.out.println("\n----- 序列化演示菜单 -----");
    System.out.println("1. 运行序列化演示");
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