package org.example.hazelcast.demo.datastructure.ap.replicatedmap;

import com.hazelcast.core.HazelcastInstance;
import org.springframework.stereotype.Component;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * Hazelcast ReplicatedMap 示例运行器
 */
@Component
public class ReplicatedMapDemoRunner {

  private final ReplicatedMapBasicOperationsDemo replicatedMapBasicOperationsDemo;
  private final HazelcastInstance hazelcastInstance;

  public ReplicatedMapDemoRunner(ReplicatedMapBasicOperationsDemo replicatedMapBasicOperationsDemo,
      HazelcastInstance hazelcastInstance) {
    this.replicatedMapBasicOperationsDemo = replicatedMapBasicOperationsDemo;
    this.hazelcastInstance = hazelcastInstance;
  }

  /**
   * 运行ReplicatedMap示例
   */
  public void replicatedMapRunner() {
    boolean exit = false;
    BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));

    while (!exit) {
      showReplicatedMapMenu();
      try {
        System.out.print("请输入选择 [0-5]: ");
        String line = reader.readLine();
        int choice = Integer.parseInt(line);

        switch (choice) {
          case 1:
            replicatedMapBasicOperationsDemo.runAllExamples();
            System.out.println("\nReplicatedMap所有示例完成！按任意键继续...");
            waitForKeyPress();
            break;
          case 2:
            replicatedMapBasicOperationsDemo.basicOperationsExample();
            System.out.println("\n基本操作示例完成！按任意键继续...");
            waitForKeyPress();
            break;
          case 3:
            replicatedMapBasicOperationsDemo.inMemoryFormatExample();
            System.out.println("\n内存格式示例完成！按任意键继续...");
            waitForKeyPress();
            break;
          case 4:
            replicatedMapBasicOperationsDemo.entryListenerExample();
            System.out.println("\n条目监听器示例完成！按任意键继续...");
            waitForKeyPress();
            break;
          case 5:
            replicatedMapBasicOperationsDemo.concurrentAccessExample();
            System.out.println("\n并发访问示例完成！按任意键继续...");
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
   * 显示ReplicatedMap菜单
   */
  private void showReplicatedMapMenu() {
    System.out.println("\n=================================================================");
    System.out.println("                Hazelcast ReplicatedMap 操作示例");
    System.out.println("=================================================================");
    System.out.println("ReplicatedMap特点: 在集群中的所有成员上复制每个条目，提供近实时的数据复制");
    System.out.println("1. 运行所有ReplicatedMap示例");
    System.out.println("2. 基本操作示例");
    System.out.println("3. 内存格式示例");
    System.out.println("4. 条目监听器示例");
    System.out.println("5. 并发访问示例");
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