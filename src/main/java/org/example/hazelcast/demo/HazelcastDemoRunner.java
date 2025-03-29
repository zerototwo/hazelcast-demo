package org.example.hazelcast.demo;

import com.hazelcast.core.HazelcastInstance;
import org.example.hazelcast.demo.cp.cpmap.CPMapDemoRunner;
import org.example.hazelcast.demo.cp.fencedlock.FencedLockDemoRunner;
import org.example.hazelcast.demo.cp.iatomiclong.IAtomicLongDemoRunner;
import org.example.hazelcast.demo.cp.iatomicreference.IAtomicReferenceDemoRunner;
import org.example.hazelcast.demo.cp.icountdownlatch.ICountDownLatchDemoRunner;
import org.example.hazelcast.demo.cp.isemaphore.ISemaphoreDemoRunner;
import org.example.hazelcast.demo.eventjournal.eventjournal.EventJournalDemoRunner;
import org.example.hazelcast.demo.ap.flakeid.FlakeIdDemoRunner;
import org.example.hazelcast.demo.ap.list.ListDemoRunner;
import org.example.hazelcast.demo.ap.map.MapDemoRunner;
import org.example.hazelcast.demo.ap.multimap.MultiMapDemoRunner;
import org.example.hazelcast.demo.ap.queue.QueueDemoRunner;
import org.example.hazelcast.demo.ap.replicatedmap.ReplicatedMapDemoRunner;
import org.example.hazelcast.demo.ap.ringbuffer.RingbufferDemoRunner;
import org.example.hazelcast.demo.ap.set.SetDemoRunner;
import org.example.hazelcast.demo.ap.topic.TopicDemoRunner;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.CommandLineRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.annotation.Order;

import java.util.Scanner;

/**
 * Hazelcast 示例应用主运行器
 * 处理所有数据结构示例的统一入口
 */
@Configuration
public class HazelcastDemoRunner {

  private final TopicDemoRunner topicDemoRunner;
  private final QueueDemoRunner queueDemoRunner;
  private final MultiMapDemoRunner multiMapDemoRunner;
  private final ReplicatedMapDemoRunner replicatedMapDemoRunner;
  private final SetDemoRunner setDemoRunner;
  private final ListDemoRunner listDemoRunner;
  private final RingbufferDemoRunner ringbufferDemoRunner;
  private final FlakeIdDemoRunner flakeIdDemoRunner;
  private final MapDemoRunner mapDemoRunner;
  private final CPMapDemoRunner cpMapDemoRunner;
  private final FencedLockDemoRunner fencedLockDemoRunner;
  private final IAtomicLongDemoRunner iAtomicLongDemoRunner;
  private final IAtomicReferenceDemoRunner iAtomicReferenceDemoRunner;
  private final ICountDownLatchDemoRunner iCountDownLatchDemoRunner;
  private final ISemaphoreDemoRunner iSemaphoreDemoRunner;
  private final EventJournalDemoRunner eventJournalDemoRunner;

  @Autowired
  public HazelcastDemoRunner(TopicDemoRunner topicDemoRunner,
      QueueDemoRunner queueDemoRunner,
      MultiMapDemoRunner multiMapDemoRunner,
      ReplicatedMapDemoRunner replicatedMapDemoRunner,
      SetDemoRunner setDemoRunner,
      ListDemoRunner listDemoRunner,
      RingbufferDemoRunner ringbufferDemoRunner,
      FlakeIdDemoRunner flakeIdDemoRunner,
      MapDemoRunner mapDemoRunner,
      CPMapDemoRunner cpMapDemoRunner,
      FencedLockDemoRunner fencedLockDemoRunner,
      IAtomicLongDemoRunner iAtomicLongDemoRunner,
      IAtomicReferenceDemoRunner iAtomicReferenceDemoRunner,
      ICountDownLatchDemoRunner iCountDownLatchDemoRunner,
      ISemaphoreDemoRunner iSemaphoreDemoRunner,
      EventJournalDemoRunner eventJournalDemoRunner) {
    this.topicDemoRunner = topicDemoRunner;
    this.queueDemoRunner = queueDemoRunner;
    this.multiMapDemoRunner = multiMapDemoRunner;
    this.replicatedMapDemoRunner = replicatedMapDemoRunner;
    this.setDemoRunner = setDemoRunner;
    this.listDemoRunner = listDemoRunner;
    this.ringbufferDemoRunner = ringbufferDemoRunner;
    this.flakeIdDemoRunner = flakeIdDemoRunner;
    this.mapDemoRunner = mapDemoRunner;
    this.cpMapDemoRunner = cpMapDemoRunner;
    this.fencedLockDemoRunner = fencedLockDemoRunner;
    this.iAtomicLongDemoRunner = iAtomicLongDemoRunner;
    this.iAtomicReferenceDemoRunner = iAtomicReferenceDemoRunner;
    this.iCountDownLatchDemoRunner = iCountDownLatchDemoRunner;
    this.iSemaphoreDemoRunner = iSemaphoreDemoRunner;
    this.eventJournalDemoRunner = eventJournalDemoRunner;
  }

  /**
   * CommandLineRunner用于在Spring Boot启动后运行示例
   */
  @Bean
  @Order(100)
  public CommandLineRunner runDemos(
      HazelcastInstance hazelcastInstance,
      @Qualifier("jcacheDemoRunner") CommandLineRunner cacheDemoRunner) {

    return args -> {
      System.out.println("\n=================================================================");
      System.out.println("                Hazelcast 演示应用");
      System.out.println("=================================================================\n");

      try {
        while (true) {
          printMainMenu();
          int choice = getUserChoice();

          switch (choice) {
            case 0:
              System.out.println("退出应用...");
              hazelcastInstance.shutdown();
              return;
            case 1:
              mapDemoRunner.mapRunner();
              break;
            case 2:
              cacheDemoRunner.run(args);
              break;
            case 3:
              topicDemoRunner.showTopicMenu();
              break;
            case 4:
              queueDemoRunner.queueRunner();
              break;
            case 5:
              multiMapDemoRunner.multiMapRunner();
              break;
            case 6:
              replicatedMapDemoRunner.replicatedMapRunner();
              break;
            case 7:
              setDemoRunner.setRunner();
              break;
            case 8:
              listDemoRunner.listRunner();
              break;
            case 9:
              ringbufferDemoRunner.ringbufferRunner();
              break;
            case 10:
              flakeIdDemoRunner.flakeIdRunner();
              break;
            case 11:
              cpMapDemoRunner.cpMapRunner();
              break;
            case 12:
              fencedLockDemoRunner.fencedLockRunner();
              break;
            case 13:
              iAtomicLongDemoRunner.iAtomicLongRunner();
              break;
            case 14:
              iAtomicReferenceDemoRunner.iAtomicReferenceRunner();
              break;
            case 15:
              iCountDownLatchDemoRunner.iCountDownLatchRunner();
              break;
            case 16:
              iSemaphoreDemoRunner.isemaphoreRunner();
              break;
            case 17:
              eventJournalDemoRunner.eventJournalRunner();
              break;
            default:
              System.out.println("无效选择，请重试。");
          }
        }
      } finally {
        hazelcastInstance.shutdown();
      }
    };
  }

  /**
   * 打印主菜单
   */
  private void printMainMenu() {
    System.out.println("\n请选择要运行的示例类型：");
    System.out.println("1. Map 操作示例");
    System.out.println("2. JCache 操作示例");
    System.out.println("3. Topic 操作示例");
    System.out.println("4. Queue 操作示例");
    System.out.println("5. MultiMap 操作示例");
    System.out.println("6. ReplicatedMap 操作示例");
    System.out.println("7. Set 操作示例");
    System.out.println("8. List 操作示例");
    System.out.println("9. Ringbuffer 操作示例");
    System.out.println("10. Flake ID Generator 操作示例");
    System.out.println("11. CPMap 操作示例 (企业版)");
    System.out.println("12. FencedLock 操作示例 (企业版)");
    System.out.println("13. IAtomicLong 操作示例");
    System.out.println("14. IAtomicReference 操作示例");
    System.out.println("15. ICountDownLatch 操作示例");
    System.out.println("16. ISemaphore 操作示例");
    System.out.println("17. Event Journal 操作示例 (企业版)");
    System.out.println("0. 退出");
    System.out.print("请输入选择 [0-17]: ");
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