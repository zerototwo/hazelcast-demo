package org.example.hazelcast.demo.ap.topic;

import com.hazelcast.core.HazelcastInstance;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Scanner;

/**
 * Hazelcast Topic 示例运行器
 */
@Configuration
public class TopicDemoRunner {

  private final TopicBasicOperationsDemo topicBasicOperationsDemo;
  private final TopicReliableMessagingDemo topicReliableMessagingDemo;
  private final HazelcastInstance hazelcastInstance;

  @Autowired
  public TopicDemoRunner(TopicBasicOperationsDemo topicBasicOperationsDemo,
      TopicReliableMessagingDemo topicReliableMessagingDemo,
      HazelcastInstance hazelcastInstance) {
    this.topicBasicOperationsDemo = topicBasicOperationsDemo;
    this.topicReliableMessagingDemo = topicReliableMessagingDemo;
    this.hazelcastInstance = hazelcastInstance;
  }

  @Bean
  public CommandLineRunner topicRunner() {
    return args -> {
      // Topic 功能在主菜单中选择后运行
    };
  }

  /**
   * 显示 Topic 示例菜单并处理用户选择
   */
  public void showTopicMenu() {
    Scanner scanner = new Scanner(System.in);
    boolean exit = false;

    while (!exit) {
      System.out.println("\n===== Hazelcast Topic 示例菜单 =====");
      System.out.println("1. 基本 Topic 操作示例");
      System.out.println("2. 可靠消息传递示例");
      System.out.println("3. 运行所有 Topic 示例");
      System.out.println("0. 返回主菜单");
      System.out.print("请选择: ");

      int choice;
      try {
        choice = Integer.parseInt(scanner.nextLine().trim());
      } catch (NumberFormatException e) {
        System.out.println("无效的选择，请重试。");
        continue;
      }

      switch (choice) {
        case 1:
          topicBasicOperationsDemo.runAllExamples();
          break;
        case 2:
          topicReliableMessagingDemo.runAllExamples();
          break;
        case 3:
          topicBasicOperationsDemo.runAllExamples();
          waitForKeyPress();
          topicReliableMessagingDemo.runAllExamples();
          break;
        case 0:
          exit = true;
          break;
        default:
          System.out.println("无效的选择，请重试。");
      }

      if (choice != 0 && choice != -1) {
        System.out.println("\nTopic示例执行完成。按回车键继续...");
        waitForKeyPress();
      }
    }
  }

  /**
   * 等待用户按键
   */
  private void waitForKeyPress() {
    try {
      System.in.read();
      // 清除输入缓冲
      while (System.in.available() > 0) {
        System.in.read();
      }
    } catch (Exception e) {
      // 忽略异常
    }
  }
}