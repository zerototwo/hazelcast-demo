package org.example.hazelcast.demo.datastructure.ap.set;

import com.hazelcast.core.HazelcastInstance;
import org.springframework.stereotype.Component;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * Hazelcast Set 示例运行器
 */
@Component
public class SetDemoRunner {

  private final SetBasicOperationsDemo setBasicOperationsDemo;
  private final HazelcastInstance hazelcastInstance;

  public SetDemoRunner(SetBasicOperationsDemo setBasicOperationsDemo,
      HazelcastInstance hazelcastInstance) {
    this.setBasicOperationsDemo = setBasicOperationsDemo;
    this.hazelcastInstance = hazelcastInstance;
  }

  /**
   * 运行Set示例
   */
  public void setRunner() {
    boolean exit = false;
    BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));

    while (!exit) {
      showSetMenu();
      try {
        System.out.print("请输入选择 [0-5]: ");
        String line = reader.readLine();
        int choice = Integer.parseInt(line);

        switch (choice) {
          case 1:
            setBasicOperationsDemo.runAllExamples();
            System.out.println("\nSet所有示例完成！按任意键继续...");
            waitForKeyPress();
            break;
          case 2:
            setBasicOperationsDemo.basicOperationsExample();
            System.out.println("\n基本操作示例完成！按任意键继续...");
            waitForKeyPress();
            break;
          case 3:
            setBasicOperationsDemo.itemListenerExample();
            System.out.println("\n项目监听器示例完成！按任意键继续...");
            waitForKeyPress();
            break;
          case 4:
            setBasicOperationsDemo.configurationExample();
            System.out.println("\n配置示例完成！按任意键继续...");
            waitForKeyPress();
            break;
          case 5:
            setBasicOperationsDemo.setOperationsExample();
            System.out.println("\nSet集合操作示例完成！按任意键继续...");
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
   * 显示Set菜单
   */
  private void showSetMenu() {
    System.out.println("\n=================================================================");
    System.out.println("                  Hazelcast Set 操作示例");
    System.out.println("=================================================================");
    System.out.println("Set特点: 分布式集合，不允许重复元素，支持丰富的配置选项");
    System.out.println("1. 运行所有Set示例");
    System.out.println("2. 基本操作示例");
    System.out.println("3. 项目监听器示例");
    System.out.println("4. 配置选项示例");
    System.out.println("5. Set集合操作示例");
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