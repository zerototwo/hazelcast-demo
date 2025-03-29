package org.example.hazelcast.demo.datastructure.ap.flakeid;

import com.hazelcast.core.HazelcastInstance;
import org.springframework.stereotype.Component;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * Hazelcast Flake ID Generator 示例运行器
 * 
 * 此类作为Flake ID Generator示例的交互式入口点，让用户能够选择并运行
 * 不同类型的示例，展示Hazelcast Flake ID Generator的各种功能和特性。
 * 
 * Flake ID Generator是Hazelcast提供的一种分布式ID生成器，它生成64位的唯一标识符，
 * 可用于多种分布式系统场景，如：
 * - 分布式主键生成
 * - 分布式事务ID
 * - 消息去重ID
 * - 时序数据ID
 * - 分布式计数器
 * 
 * 本示例运行器提供用户友好的菜单界面，允许用户探索Flake ID的基本操作和高级配置，
 * 以便更好地理解和应用该功能。
 */
@Component
public class FlakeIdDemoRunner {

  private final FlakeIdBasicOperationsDemo basicOperationsDemo;
  private final FlakeIdConfigDemo configDemo;
  private final HazelcastInstance hazelcastInstance;

  /**
   * 构造函数，注入所需的依赖组件
   * 
   * @param basicOperationsDemo Flake ID基本操作示例组件
   * @param configDemo          Flake ID配置示例组件
   * @param hazelcastInstance   Hazelcast实例，用于访问Hazelcast服务
   */
  public FlakeIdDemoRunner(FlakeIdBasicOperationsDemo basicOperationsDemo,
      FlakeIdConfigDemo configDemo,
      HazelcastInstance hazelcastInstance) {
    this.basicOperationsDemo = basicOperationsDemo;
    this.configDemo = configDemo;
    this.hazelcastInstance = hazelcastInstance;
  }

  /**
   * Flake ID示例运行器的主方法
   * 
   * 显示交互式菜单，根据用户输入选择并运行相应的示例。
   * 支持运行所有示例或选择特定示例进行演示。
   * 包含异常处理机制，确保用户操作错误不会导致程序崩溃。
   */
  public void flakeIdRunner() {
    // 创建控制台输入读取器
    BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
    boolean exit = false;

    while (!exit) {
      // 显示菜单选项
      showFlakeIdMenu();
      System.out.print("请选择操作[1-4]: ");

      try {
        // 读取并处理用户输入
        String input = reader.readLine();
        System.out.println(); // 添加空行提高可读性

        // 根据用户选择执行相应操作
        switch (input) {
          case "1": // 运行所有示例
            basicOperationsDemo.runAllExamples();
            configDemo.runAllExamples();
            waitForKeyPress(reader);
            break;
          case "2": // 运行基本操作示例
            basicOperationsDemo.runAllExamples();
            waitForKeyPress(reader);
            break;
          case "3": // 运行配置选项示例
            configDemo.runAllExamples();
            waitForKeyPress(reader);
            break;
          case "4": // 退出菜单
            exit = true;
            break;
          default: // 处理无效输入
            System.out.println("无效选择，请重试");
            break;
        }
      } catch (IOException e) {
        // 处理IO异常
        System.out.println("错误: " + e.getMessage());
      }
    }
  }

  /**
   * 显示Flake ID Generator示例菜单
   * 
   * 向用户展示可用的操作选项，包括运行完整示例集、
   * 基本操作示例、配置选项示例，以及退出选项。
   */
  private void showFlakeIdMenu() {
    System.out.println("\n==================================");
    System.out.println("   Flake ID Generator 示例");
    System.out.println("==================================");
    System.out.println("1. 运行所有示例");
    System.out.println("2. 运行基本操作示例");
    System.out.println("3. 运行配置选项示例");
    System.out.println("4. 退出菜单");
    System.out.println("==================================");
  }

  /**
   * 等待用户按键继续
   * 
   * 提示用户按下回车键继续程序执行，用于在示例之间创建暂停，
   * 让用户有时间阅读输出内容，提升用户体验。
   * 
   * @param reader 控制台输入读取器
   * @throws IOException 如果读取输入时发生IO错误
   */
  private void waitForKeyPress(BufferedReader reader) throws IOException {
    System.out.println("\n按回车键继续...");
    reader.readLine();
  }
}