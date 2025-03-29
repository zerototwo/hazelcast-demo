package org.example.hazelcast.demo.compute.executor;

import com.hazelcast.core.HazelcastInstance;
import org.springframework.stereotype.Component;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * 用于运行ExecutorService示例的运行器类
 * 提供一个交互式菜单，让用户选择要运行的ExecutorService示例
 */
@Component
public class ExecutorServiceDemoRunner {

  private final ExecutorServiceDemo executorServiceDemo;
  private final HazelcastInstance hazelcastInstance;
  private final BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));

  public ExecutorServiceDemoRunner(ExecutorServiceDemo executorServiceDemo, HazelcastInstance hazelcastInstance) {
    this.executorServiceDemo = executorServiceDemo;
    this.hazelcastInstance = hazelcastInstance;
  }

  /**
   * 入口方法，展示菜单并处理用户输入
   */
  public void executorServiceRunner() {
    int choice;
    do {
      showExecutorServiceMenu();
      choice = getUserChoice();

      switch (choice) {
        case 1:
          executorServiceDemo.runAllExamples();
          waitForKeyPress();
          break;
        case 2:
          executorServiceDemo.basicExecutorExample();
          waitForKeyPress();
          break;
        case 3:
          executorServiceDemo.executeOnAllMembersExample();
          waitForKeyPress();
          break;
        case 4:
          executorServiceDemo.executeOnKeyOwnerExample();
          waitForKeyPress();
          break;
        case 5:
          executorServiceDemo.executeWithCallbackExample();
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
   * 显示ExecutorService菜单选项
   */
  private void showExecutorServiceMenu() {
    System.out.println("\n==== Hazelcast ExecutorService示例 ====");
    System.out.println("1. 运行所有ExecutorService示例");
    System.out.println("2. 基本执行器示例");
    System.out.println("3. 在所有成员上执行的示例");
    System.out.println("4. 在键所有者上执行的示例");
    System.out.println("5. 使用回调的示例");
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