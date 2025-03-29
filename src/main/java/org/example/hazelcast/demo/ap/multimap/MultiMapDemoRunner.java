package org.example.hazelcast.demo.ap.multimap;

import com.hazelcast.core.HazelcastInstance;
import org.springframework.stereotype.Component;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * Hazelcast MultiMap 示例运行器
 */
@Component
public class MultiMapDemoRunner {

  private final MultiMapBasicOperationsDemo multiMapBasicOperationsDemo;
  private final HazelcastInstance hazelcastInstance;

  public MultiMapDemoRunner(MultiMapBasicOperationsDemo multiMapBasicOperationsDemo,
      HazelcastInstance hazelcastInstance) {
    this.multiMapBasicOperationsDemo = multiMapBasicOperationsDemo;
    this.hazelcastInstance = hazelcastInstance;
  }

  /**
   * 运行MultiMap示例
   */
  public void multiMapRunner() {
    boolean exit = false;
    BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));

    while (!exit) {
      showMultiMapMenu();
      try {
        System.out.print("请输入选择 [0-2]: ");
        String line = reader.readLine();
        int choice = Integer.parseInt(line);

        switch (choice) {
          case 1:
            multiMapBasicOperationsDemo.runAllExamples();
            System.out.println("\nMultiMap示例完成！按任意键继续...");
            waitForKeyPress();
            break;
          case 2:
            // 单独运行各个示例
            System.out.println("\n=== 基本操作示例 ===");
            multiMapBasicOperationsDemo.basicOperationsExample();
            System.out.println("\n基本操作示例完成！按任意键继续...");
            waitForKeyPress();

            System.out.println("\n=== 集合类型示例 ===");
            multiMapBasicOperationsDemo.collectionTypeExample();
            System.out.println("\n集合类型示例完成！按任意键继续...");
            waitForKeyPress();

            System.out.println("\n=== 锁定示例 ===");
            multiMapBasicOperationsDemo.lockingExample();
            System.out.println("\n锁定示例完成！按任意键继续...");
            waitForKeyPress();

            System.out.println("\n=== 监听器示例 ===");
            multiMapBasicOperationsDemo.listenersExample();
            System.out.println("\n监听器示例完成！按任意键继续...");
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
   * 显示MultiMap菜单
   */
  private void showMultiMapMenu() {
    System.out.println("\n=================================================================");
    System.out.println("                  Hazelcast MultiMap 操作示例");
    System.out.println("=================================================================");
    System.out.println("1. 运行所有MultiMap示例");
    System.out.println("2. 分步运行各个示例");
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