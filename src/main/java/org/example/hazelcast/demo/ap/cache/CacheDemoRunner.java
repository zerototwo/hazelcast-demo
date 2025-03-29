package org.example.hazelcast.demo.ap.cache;

import com.hazelcast.core.HazelcastInstance;
import org.springframework.boot.CommandLineRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Scanner;

/**
 * Hazelcast JCache 操作示例运行器
 */
@Configuration
public class CacheDemoRunner {

  /**
   * CommandLineRunner用于在Spring Boot启动后运行JCache示例
   */
  @Bean(name = "jcacheDemoRunner")
  public CommandLineRunner runJCacheDemos(
      HazelcastInstance hazelcastInstance,
      CacheBasicOperationsDemo basicOperationsDemo,
      CacheEntryProcessorDemo entryProcessorDemo,
      CacheListenersDemo listenersDemo) {

    return args -> {
      System.out.println("\n=================================================================");
      System.out.println("                Hazelcast JCache 操作示例应用");
      System.out.println("=================================================================\n");

      try {
        while (true) {
          printMenu();
          int choice = getUserChoice();

          switch (choice) {
            case 0:
              System.out.println("退出JCache示例应用...");
              return;
            case 1:
              basicOperationsDemo.runAllExamples();
              break;
            case 2:
              entryProcessorDemo.runAllExamples();
              break;
            case 3:
              listenersDemo.runAllExamples();
              break;
            case 4:
              // 运行所有示例
              basicOperationsDemo.runAllExamples();
              waitForKeyPress();

              entryProcessorDemo.runAllExamples();
              waitForKeyPress();

              listenersDemo.runAllExamples();
              break;
            default:
              System.out.println("无效选择，请重试。");
          }

          if (choice != 0) {
            System.out.println("\nJCache示例执行完成。按回车键继续...");
            waitForKeyPress();
          }
        }
      } finally {
        // 由于使用的是共享的HazelcastInstance，不在这里关闭它
      }
    };
  }

  /**
   * 打印主菜单
   */
  private void printMenu() {
    System.out.println("\n请选择要运行的JCache示例：");
    System.out.println("1. 基本缓存操作示例");
    System.out.println("2. 缓存EntryProcessor示例");
    System.out.println("3. 缓存监听器示例");
    System.out.println("4. 运行所有示例");
    System.out.println("0. 返回主菜单");
    System.out.print("请输入选择 [0-4]: ");
  }

  /**
   * 获取用户输入
   */
  private int getUserChoice() {
    Scanner scanner = new Scanner(System.in);
    try {
      return scanner.nextInt();
    } catch (Exception e) {
      return -1;
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