package org.example.hazelcast.demo.datastructure.ap.topic;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.topic.ITopic;
import com.hazelcast.topic.Message;
import com.hazelcast.topic.MessageListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Hazelcast Topic 基本操作示例
 * 
 * 官方定义：
 * Hazelcast Topic是一种分布式发布/订阅(pub/sub)消息系统，允许应用程序之间进行异步通信。
 * 它实现了发布-订阅模式，其中消息的发布者和消息的订阅者是解耦的。
 * 
 * 主要特性：
 * 1. 发布/订阅模式：一个发布者可以发送消息给多个订阅者
 * 2. 异步通信：发布者不需要等待订阅者处理完消息
 * 3. 集群范围的传播：消息发送到集群中的所有节点
 * 4. 可靠消息传递：支持可靠的消息传递，确保消息不会丢失
 * 5. 全局顺序：保证在单个集群节点上消息的发布顺序与接收顺序一致
 * 6. 灵活订阅：支持动态添加和移除消息监听器
 * 7. 本地订阅：可以选择仅接收本地发布的消息
 * 8. 统计收集：支持消息发送统计信息
 * 
 * 两种Topic类型：
 * 1. 普通Topic：标准的发布/订阅实现，不保存历史消息
 * 2. 可靠Topic：使用Ringbuffer存储消息，保证消息可靠传递，支持重放
 * 
 * 适用场景：
 * - 事件通知系统
 * - 实时数据分发
 * - 系统监控和警报
 * - 聊天和消息应用
 * - 配置更新通知
 * - 集群状态广播
 * - 分布式缓存失效通知
 * 
 * Topic与其他消息传递机制的区别：
 * - 与Queue区别：Topic广播给所有订阅者，Queue中的消息只由一个消费者处理
 * - 与Event System区别：Topic专注于跨节点的消息传递，Event System主要用于单节点内的事件
 * - 与JMS Topics区别：Hazelcast Topic是为分布式环境优化的实现
 * 
 * 性能考虑：
 * - 发布操作会将消息发送到所有集群成员，可能导致网络负载
 * - 可靠Topic需要更多的内存来存储消息历史
 * - 消息处理通常是异步的，以提高吞吐量
 * 
 * 本示例演示了Hazelcast Topic的基本操作，包括创建、发布消息、
 * 订阅消息、监听器管理和消息处理等功能。
 */
@Component
public class TopicBasicOperationsDemo {

  private final HazelcastInstance hazelcastInstance;
  private final String TOPIC_NAME = "demo-topic";

  // 使用日期时间格式化器
  private final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");

  @Autowired
  public TopicBasicOperationsDemo(HazelcastInstance hazelcastInstance) {
    this.hazelcastInstance = hazelcastInstance;
  }

  /**
   * 运行所有基本操作示例
   */
  public void runAllExamples() {
    System.out.println("=================== Hazelcast Topic 基本操作示例 ===================");

    basicPublishSubscribeExample();
    multipleSubscribersExample();
    messageOrderingExample();
    topicStatisticsExample();
  }

  /**
   * 基本发布/订阅示例
   */
  public void basicPublishSubscribeExample() {
    System.out.println("\n--- 基本发布/订阅示例 ---");

    // 创建Topic
    ITopic<String> topic = hazelcastInstance.getTopic(TOPIC_NAME);

    // 创建一个CountDownLatch来等待消息接收
    CountDownLatch latch = new CountDownLatch(1);

    // 注册监听器
    UUID listenerId = topic.addMessageListener(new MessageListener<String>() {
      @Override
      public void onMessage(Message<String> message) {
        System.out.println("收到消息: " + message.getMessageObject());
        System.out.println("发布时间: " + message.getPublishTime());
        System.out.println("发布成员: " + message.getPublishingMember());
        latch.countDown();
      }
    });

    System.out.println("已注册消息监听器，ID: " + listenerId);

    // 发布消息
    String messageText = "Hello Hazelcast Topic! - " + LocalDateTime.now().format(formatter);
    topic.publish(messageText);
    System.out.println("已发布消息: " + messageText);

    try {
      // 等待消息被接收
      boolean received = latch.await(5, TimeUnit.SECONDS);
      if (!received) {
        System.out.println("警告: 等待接收消息超时");
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      System.err.println("等待接收消息时被中断: " + e.getMessage());
    }

    // 移除监听器
    topic.removeMessageListener(listenerId);
    System.out.println("已移除消息监听器");
  }

  /**
   * 多订阅者示例
   */
  public void multipleSubscribersExample() {
    System.out.println("\n--- 多订阅者示例 ---");

    // 创建Topic
    ITopic<String> topic = hazelcastInstance.getTopic(TOPIC_NAME);

    // 创建一个CountDownLatch来等待所有消息接收
    CountDownLatch latch = new CountDownLatch(2);

    // 注册第一个监听器
    UUID listenerId1 = topic.addMessageListener(new MessageListener<String>() {
      @Override
      public void onMessage(Message<String> message) {
        System.out.println("订阅者1收到消息: " + message.getMessageObject());
        latch.countDown();
      }
    });

    // 注册第二个监听器
    UUID listenerId2 = topic.addMessageListener(new MessageListener<String>() {
      @Override
      public void onMessage(Message<String> message) {
        System.out.println("订阅者2收到消息: " + message.getMessageObject());
        latch.countDown();
      }
    });

    System.out.println("已注册两个消息监听器");

    // 发布消息
    String messageText = "这条消息会被两个订阅者接收 - " + LocalDateTime.now().format(formatter);
    topic.publish(messageText);
    System.out.println("已发布消息: " + messageText);

    try {
      // 等待所有消息被接收
      boolean allReceived = latch.await(5, TimeUnit.SECONDS);
      if (!allReceived) {
        System.out.println("警告: 等待接收消息超时");
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      System.err.println("等待接收消息时被中断: " + e.getMessage());
    }

    // 移除监听器
    topic.removeMessageListener(listenerId1);
    topic.removeMessageListener(listenerId2);
    System.out.println("已移除所有消息监听器");
  }

  /**
   * 消息顺序示例
   */
  public void messageOrderingExample() {
    System.out.println("\n--- 消息顺序示例 ---");

    // 创建Topic
    ITopic<String> topic = hazelcastInstance.getTopic(TOPIC_NAME);

    // 创建有序消息跟踪器
    OrderedMessageTracker tracker = new OrderedMessageTracker(5);

    // 注册监听器
    UUID listenerId = topic.addMessageListener(tracker);
    System.out.println("已注册消息顺序跟踪监听器");

    // 发布多条消息
    for (int i = 1; i <= 5; i++) {
      String messageText = "顺序消息 #" + i;
      topic.publish(messageText);
      System.out.println("已发布: " + messageText);

      // 短暂暂停，确保消息按顺序发布
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }

    try {
      // 等待所有消息被接收
      boolean allReceived = tracker.waitForAllMessages(5, TimeUnit.SECONDS);
      if (allReceived) {
        System.out.println("所有消息按照以下顺序接收:");
        System.out.println(tracker.getReceivedMessages());
      } else {
        System.out.println("警告: 等待接收消息超时");
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      System.err.println("等待接收消息时被中断: " + e.getMessage());
    }

    // 移除监听器
    topic.removeMessageListener(listenerId);
    System.out.println("已移除消息监听器");
  }

  /**
   * Topic统计信息示例
   */
  public void topicStatisticsExample() {
    System.out.println("\n--- Topic统计信息示例 ---");

    // 创建Topic
    ITopic<String> topic = hazelcastInstance.getTopic(TOPIC_NAME);

    // 注册监听器
    UUID listenerId = topic.addMessageListener(message -> {
      System.out.println("统计示例 - 收到消息: " + message.getMessageObject());
    });

    // 显示初始统计信息
    System.out.println("初始发布操作计数: " + topic.getLocalTopicStats().getPublishOperationCount());
    System.out.println("初始接收操作计数: " + topic.getLocalTopicStats().getReceiveOperationCount());

    // 发布多条消息
    for (int i = 1; i <= 5; i++) {
      topic.publish("统计测试消息 #" + i);
    }

    // 显示更新后的统计信息
    System.out.println("发布5条消息后:");
    System.out.println("发布操作计数: " + topic.getLocalTopicStats().getPublishOperationCount());
    System.out.println("接收操作计数: " + topic.getLocalTopicStats().getReceiveOperationCount());

    // 移除监听器
    topic.removeMessageListener(listenerId);
    System.out.println("已移除消息监听器");
  }

  /**
   * 有序消息跟踪器
   */
  private static class OrderedMessageTracker implements MessageListener<String>, Serializable {
    private static final long serialVersionUID = 1L;

    private final CountDownLatch latch;
    private final StringBuilder receivedMessages = new StringBuilder();

    public OrderedMessageTracker(int expectedMessageCount) {
      this.latch = new CountDownLatch(expectedMessageCount);
    }

    @Override
    public void onMessage(Message<String> message) {
      receivedMessages.append(message.getMessageObject()).append("\n");
      latch.countDown();
    }

    public boolean waitForAllMessages(long timeout, TimeUnit unit) throws InterruptedException {
      return latch.await(timeout, unit);
    }

    public String getReceivedMessages() {
      return receivedMessages.toString();
    }
  }
}