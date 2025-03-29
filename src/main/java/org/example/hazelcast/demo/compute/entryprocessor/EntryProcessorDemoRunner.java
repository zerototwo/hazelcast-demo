package org.example.hazelcast.demo.compute.entryprocessor;

import com.hazelcast.core.HazelcastInstance;
import org.springframework.stereotype.Component;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * 用于运行EntryProcessor示例的运行器类
 * 提供一个交互式菜单，让用户选择要运行的EntryProcessor示例
 */
@Component
public class EntryProcessorDemoRunner {

  private final EntryProcessorDemo entryProcessorDemo;
  private final HazelcastInstance hazelcastInstance;
  private final BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));

  public EntryProcessorDemoRunner(EntryProcessorDemo entryProcessorDemo, HazelcastInstance hazelcastInstance) {
    this.entryProcessorDemo = entryProcessorDemo;
    this.hazelcastInstance = hazelcastInstance;
  }

  /**
   * 入口方法，展示菜单并处理用户输入
   */
  public void entryProcessorRunner() {
    int choice;
    do {
      showEntryProcessorMenu();
      choice = getUserChoice();

      switch (choice) {
        case 1:
          entryProcessorDemo.runAllExamples();
          waitForKeyPress();
          break;
        case 2:
          entryProcessorDemo.basicEntryProcessorExample();
          waitForKeyPress();
          break;
        case 3:
          entryProcessorDemo.batchEntryProcessorExample();
          waitForKeyPress();
          break;
        case 4:
          entryProcessorDemo.conditionalUpdateExample();
          waitForKeyPress();
          break;
        case 5:
          entryProcessorDemo.returningResultsExample();
          waitForKeyPress();
          break;
        case 0:
          System.out.println("返回主菜单...");
          break;
        default:
          System.out.println("无效选择，请重试");
      }
    } while (choice != 0);
  }

  /**
   * 显示EntryProcessor菜单选项
   */
  private void showEntryProcessorMenu() {
    System.out.println("\n==== Hazelcast EntryProcessor示例 ====");
    System.out.println("1. 运行所有EntryProcessor示例");
    System.out.println("2. 基本EntryProcessor示例");
    System.out.println("3. 批量EntryProcessor示例");
    System.out.println("4. 条件更新示例");
    System.out.println("5. 返回结果示例");
    System.out.println("0. 返回主菜单");
    System.out.print("请选择 (0-5): ");
  }

  /**
   * 获取用户输入
   */
  private int getUserChoice() {
    try {
      String input = reader.readLine();
      return Integer.parseInt(input);
    } catch (IOException | NumberFormatException e) {
      return -1; // 返回无效值表示输入错误
    }
  }

  /**
   * 等待用户按键继续
   */
  private void waitForKeyPress() {
    System.out.println("\n按回车键继续...");
    try {
      reader.readLine();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}