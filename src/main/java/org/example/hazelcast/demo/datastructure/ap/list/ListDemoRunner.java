package org.example.hazelcast.demo.datastructure.ap.list;

import com.hazelcast.core.HazelcastInstance;
import org.springframework.stereotype.Component;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * Hazelcast List 示例运行器
 */
@Component
public class ListDemoRunner {

  private final ListBasicOperationsDemo listBasicOperationsDemo;
  private final HazelcastInstance hazelcastInstance;

  public ListDemoRunner(ListBasicOperationsDemo listBasicOperationsDemo,
      HazelcastInstance hazelcastInstance) {
    this.listBasicOperationsDemo = listBasicOperationsDemo;
    this.hazelcastInstance = hazelcastInstance;
  }

  /**
   * 运行List示例
   */
  public void listRunner() {
    boolean exit = false;
    BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));

    while (!exit) {
      showListMenu();
      try {
        System.out.print("请输入选择 [0-6]: ");
        String line = reader.readLine();
        int choice = Integer.parseInt(line);

        switch (choice) {
          case 1:
            listBasicOperationsDemo.runAllExamples();
            System.out.println("\nList所有示例完成！按任意键继续...");
            waitForKeyPress();
            break;
          case 2:
            listBasicOperationsDemo.basicOperationsExample();
            System.out.println("\n基本操作示例完成！按任意键继续...");
            waitForKeyPress();
            break;
          case 3:
            listBasicOperationsDemo.positionOperationsExample();
            System.out.println("\n位置操作示例完成！按任意键继续...");
            waitForKeyPress();
            break;
          case 4:
            listBasicOperationsDemo.itemListenerExample();
            System.out.println("\n项目监听器示例完成！按任意键继续...");
            waitForKeyPress();
            break;
          case 5:
            listBasicOperationsDemo.configurationExample();
            System.out.println("\n配置示例完成！按任意键继续...");
            waitForKeyPress();
            break;
          case 6:
            listBasicOperationsDemo.subListExample();
            System.out.println("\n子列表操作示例完成！按任意键继续...");
            waitForKeyPress();
            break;
          case 0:
            exit = true;
            break;
          default:
            System.out.println("无效选择，请重试。");
        }
      } catch (IOException e) {
        System.err.println("读取输入失败: " + e.getMessage());
      } catch (NumberFormatException e) {
        System.err.println("请输入有效的数字。");
      }
    }
  }

  /**
   * 显示List菜单
   */
  private void showListMenu() {
    System.out.println("\n=================================================================");
    System.out.println("                  Hazelcast List 操作示例");
    System.out.println("=================================================================");
    System.out.println("List特点: 分布式有序集合，可以包含重复元素，并提供位置相关的操作");
    System.out.println("1. 运行所有List示例");
    System.out.println("2. 基本操作示例");
    System.out.println("3. 位置操作示例");
    System.out.println("4. 项目监听器示例");
    System.out.println("5. 配置选项示例");
    System.out.println("6. 子列表操作示例");
    System.out.println("0. 返回主菜单");
  }

  /**
   * 等待用户按键
   */
  private void waitForKeyPress() {
    BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
    try {
      reader.readLine();
    } catch (IOException e) {
      System.err.println("等待输入时出错: " + e.getMessage());
    }
  }
}