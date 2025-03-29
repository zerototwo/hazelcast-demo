package org.example.hazelcast.demo.ap.ringbuffer;

import com.hazelcast.core.HazelcastInstance;
import org.springframework.stereotype.Component;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * Hazelcast Ringbuffer 示例运行器
 */
@Component
public class RingbufferDemoRunner {

  private final RingbufferBasicOperationsDemo basicOperationsDemo;
  private final RingbufferConfigDemo configDemo;
  private final HazelcastInstance hazelcastInstance;

  public RingbufferDemoRunner(
      RingbufferBasicOperationsDemo basicOperationsDemo,
      RingbufferConfigDemo configDemo,
      HazelcastInstance hazelcastInstance) {
    this.basicOperationsDemo = basicOperationsDemo;
    this.configDemo = configDemo;
    this.hazelcastInstance = hazelcastInstance;
  }

  /**
   * 运行Ringbuffer示例
   */
  public void ringbufferRunner() {
    boolean exit = false;
    BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));

    while (!exit) {
      showRingbufferMenu();
      try {
        System.out.print("请输入选择 [0-3]: ");
        String line = reader.readLine();
        int choice = Integer.parseInt(line);

        switch (choice) {
          case 1:
            basicOperationsDemo.runAllExamples();
            System.out.println("\nRingbuffer 基本操作示例完成！按任意键继续...");
            waitForKeyPress();
            break;
          case 2:
            configDemo.runAllExamples();
            System.out.println("\nRingbuffer 配置示例完成！按任意键继续...");
            waitForKeyPress();
            break;
          case 3:
            System.out.println("运行所有 Ringbuffer 示例...\n");

            basicOperationsDemo.runAllExamples();
            System.out.println("\nRingbuffer 基本操作示例完成！按任意键继续...");
            waitForKeyPress();

            configDemo.runAllExamples();
            System.out.println("\nRingbuffer 配置示例完成！按任意键继续...");
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
   * 显示Ringbuffer菜单
   */
  private void showRingbufferMenu() {
    System.out.println("\n=================================================================");
    System.out.println("                  Hazelcast Ringbuffer 操作示例");
    System.out.println("=================================================================");
    System.out.println("1. 基本操作示例 (增删改查、溢出策略、批处理、异步操作、过滤器等)");
    System.out.println("2. 配置相关示例 (容量、备份、内存格式、TTL等)");
    System.out.println("3. 运行所有 Ringbuffer 示例");
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