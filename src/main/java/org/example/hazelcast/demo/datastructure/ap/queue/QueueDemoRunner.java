package org.example.hazelcast.demo.datastructure.ap.queue;

import com.hazelcast.core.HazelcastInstance;
import org.springframework.context.annotation.Configuration;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * Hazelcast Queue 示例运行器
 */
@Configuration
public class QueueDemoRunner {

  private final QueueBasicOperationsDemo queueBasicOperationsDemo;
  private final QueueBoundedDemo queueBoundedDemo;
  private final QueuePriorityDemo queuePriorityDemo;
  private final HazelcastInstance hazelcastInstance;

  public QueueDemoRunner(QueueBasicOperationsDemo queueBasicOperationsDemo,
      QueueBoundedDemo queueBoundedDemo,
      QueuePriorityDemo queuePriorityDemo,
      HazelcastInstance hazelcastInstance) {
    this.queueBasicOperationsDemo = queueBasicOperationsDemo;
    this.queueBoundedDemo = queueBoundedDemo;
    this.queuePriorityDemo = queuePriorityDemo;
    this.hazelcastInstance = hazelcastInstance;
  }

  public void queueRunner() {
    boolean exit = false;
    BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));

    while (!exit) {
      showQueueMenu();
      try {
        System.out.print("请输入选择 [0-4]: ");
        String line = reader.readLine();
        int choice = Integer.parseInt(line);

        switch (choice) {
          case 1:
            queueBasicOperationsDemo.runAllExamples();
            System.out.println("\n基本队列示例完成！按任意键继续...");
            waitForKeyPress();
            break;
          case 2:
            queueBoundedDemo.runAllExamples();
            System.out.println("\n有界队列示例完成！按任意键继续...");
            waitForKeyPress();
            break;
          case 3:
            queuePriorityDemo.runAllExamples();
            System.out.println("\n优先级队列示例完成！按任意键继续...");
            waitForKeyPress();
            break;
          case 4:
            System.out.println("运行所有队列示例...\n");

            queueBasicOperationsDemo.runAllExamples();
            System.out.println("\n基本队列示例完成！按任意键继续...");
            waitForKeyPress();

            queueBoundedDemo.runAllExamples();
            System.out.println("\n有界队列示例完成！按任意键继续...");
            waitForKeyPress();

            queuePriorityDemo.runAllExamples();
            System.out.println("\n优先级队列示例完成！按任意键继续...");
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

  private void showQueueMenu() {
    System.out.println("\n=================================================================");
    System.out.println("                  Hazelcast Queue 操作示例");
    System.out.println("=================================================================");
    System.out.println("1. 基本队列操作示例");
    System.out.println("2. 有界队列示例");
    System.out.println("3. 优先级队列示例");
    System.out.println("4. 运行所有队列示例");
    System.out.println("0. 返回主菜单");
  }

  private void waitForKeyPress() {
    BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
    try {
      reader.readLine();
    } catch (IOException e) {
      System.err.println("等待输入时出错: " + e.getMessage());
    }
  }
}