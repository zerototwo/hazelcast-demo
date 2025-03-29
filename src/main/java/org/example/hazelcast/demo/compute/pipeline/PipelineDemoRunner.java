package org.example.hazelcast.demo.compute.pipeline;

import com.hazelcast.core.HazelcastInstance;
import org.springframework.stereotype.Component;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * 用于运行Pipeline示例的运行器类
 * 提供一个交互式菜单，让用户选择要运行的Pipeline示例
 */
@Component
public class PipelineDemoRunner {

  private final PipelineDemo pipelineDemo;
  private final HazelcastInstance hazelcastInstance;
  private final BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));

  public PipelineDemoRunner(PipelineDemo pipelineDemo, HazelcastInstance hazelcastInstance) {
    this.pipelineDemo = pipelineDemo;
    this.hazelcastInstance = hazelcastInstance;
  }

  /**
   * 入口方法，展示菜单并处理用户输入
   */
  public void pipelineRunner() {
    int choice;
    do {
      showPipelineMenu();
      choice = getUserChoice();

      switch (choice) {
        case 1:
          pipelineDemo.runAllExamples();
          waitForKeyPress();
          break;
        case 2:
          pipelineDemo.basicPipelineExample();
          waitForKeyPress();
          break;
        case 3:
          pipelineDemo.filterMapPipelineExample();
          waitForKeyPress();
          break;
        case 4:
          pipelineDemo.aggregationPipelineExample();
          waitForKeyPress();
          break;
        case 5:
          pipelineDemo.joinPipelineExample();
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
   * 显示Pipeline菜单选项
   */
  private void showPipelineMenu() {
    System.out.println("\n==== Hazelcast Pipeline示例 ====");
    System.out.println("1. 运行所有Pipeline示例");
    System.out.println("2. 基本Pipeline示例");
    System.out.println("3. 过滤和映射Pipeline示例");
    System.out.println("4. 聚合Pipeline示例");
    System.out.println("5. 连接(Join)Pipeline示例");
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