package org.example.hazelcast.demo.datastructure.ap.topic;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.topic.ITopic;
import com.hazelcast.topic.Message;
import com.hazelcast.topic.MessageListener;
import com.hazelcast.topic.ReliableMessageListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Hazelcast Topic 可靠消息传递示例
 */
@Component
public class TopicReliableMessagingDemo {

  private final HazelcastInstance hazelcastInstance;
  private final String RELIABLE_TOPIC_NAME = "reliable-demo-topic";

  @Autowired
  public TopicReliableMessagingDemo(HazelcastInstance hazelcastInstance) {
    this.hazelcastInstance = hazelcastInstance;
  }

  /**
   * 运行所有可靠消息示例
   */
  public void runAllExamples() {
    System.out.println("\n=================== Hazelcast Topic 可靠消息传递示例 ===================");

    reliableMessageListenerExample();
    messageSequenceExample();
    messageReplayExample();
  }

  /**
   * 可靠消息监听器示例
   */
  public void reliableMessageListenerExample() {
    System.out.println("\n--- 可靠消息监听器示例 ---");

    // 创建可靠Topic
    ITopic<String> reliableTopic = hazelcastInstance.getReliableTopic(RELIABLE_TOPIC_NAME);

    // 创建一个CountDownLatch来等待消息接收
    CountDownLatch latch = new CountDownLatch(5);

    // 注册可靠消息监听器
    UUID listenerId = reliableTopic.addMessageListener(new ReliableMessageListener<String>() {
      private long sequence = -1;

      @Override
      public void onMessage(Message<String> message) {
        System.out.println("收到消息: " + message.getMessageObject());
        System.out.println("  发布时间: " + message.getPublishTime());
        System.out.println("  发布成员: " + message.getPublishingMember());
        latch.countDown();
      }

      @Override
      public long retrieveInitialSequence() {
        return -1; // 从最新消息开始
      }

      @Override
      public void storeSequence(long sequence) {
        this.sequence = sequence;
        System.out.println("  存储序列: " + sequence);
      }

      @Override
      public boolean isLossTolerant() {
        return true;
      }

      @Override
      public boolean isTerminal(Throwable failure) {
        if (failure != null) {
          System.err.println("处理消息失败: " + failure.getMessage());
          failure.printStackTrace();
        }
        return false;
      }
    });

    System.out.println("已注册可靠消息监听器，ID: " + listenerId);

    // 发布多条消息
    for (int i = 1; i <= 5; i++) {
      String messageText = "可靠消息 #" + i;
      reliableTopic.publish(messageText);
      System.out.println("已发布: " + messageText);
    }

    try {
      // 等待所有消息被接收
      boolean allReceived = latch.await(10, TimeUnit.SECONDS);
      if (!allReceived) {
        System.out.println("警告: 等待接收消息超时");
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      System.err.println("等待接收消息时被中断: " + e.getMessage());
    }

    // 移除监听器
    reliableTopic.removeMessageListener(listenerId);
    System.out.println("已移除消息监听器");
  }

  /**
   * 消息序列示例
   */
  public void messageSequenceExample() {
    System.out.println("\n--- 消息序列示例 ---");

    // 创建可靠Topic
    ITopic<String> reliableTopic = hazelcastInstance.getReliableTopic(RELIABLE_TOPIC_NAME);

    // 创建一个计数锁存器来等待消息接收
    CountDownLatch latch = new CountDownLatch(3);
    AtomicLong lastSequence = new AtomicLong(-1);

    // 注册可靠消息监听器
    UUID listenerId = reliableTopic.addMessageListener(new ReliableMessageListener<String>() {
      private long sequence = -1;

      @Override
      public void onMessage(Message<String> message) {
        System.out.println("收到消息: " + message.getMessageObject());
        System.out.println("  当前序列: " + sequence);
        lastSequence.set(sequence);
        latch.countDown();
      }

      @Override
      public long retrieveInitialSequence() {
        return -1; // 从最新消息开始
      }

      @Override
      public void storeSequence(long sequence) {
        this.sequence = sequence;
      }

      @Override
      public boolean isLossTolerant() {
        return false;
      }

      @Override
      public boolean isTerminal(Throwable failure) {
        return failure != null;
      }
    });

    // 发布多条消息
    for (int i = 1; i <= 3; i++) {
      String messageText = "序列消息 #" + i;
      reliableTopic.publish(messageText);
      System.out.println("已发布: " + messageText);
    }

    try {
      // 等待所有消息被接收
      latch.await(5, TimeUnit.SECONDS);
      System.out.println("最后接收到的消息序列: " + lastSequence.get());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }

    // 移除监听器
    reliableTopic.removeMessageListener(listenerId);
  }

  /**
   * 消息重放示例
   */
  public void messageReplayExample() {
    System.out.println("\n--- 消息重放示例 ---");

    // 创建可靠Topic
    ITopic<String> reliableTopic = hazelcastInstance.getReliableTopic(RELIABLE_TOPIC_NAME);

    // 首先发布几条消息
    for (int i = 1; i <= 5; i++) {
      String messageText = "重放消息 #" + i;
      reliableTopic.publish(messageText);
      System.out.println("已发布: " + messageText);
    }

    // 延迟一小段时间，确保消息已经发布
    try {
      Thread.sleep(1000);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }

    System.out.println("\n注册从头开始的监听器...");

    // 创建一个计数锁存器来等待消息接收
    CountDownLatch latch = new CountDownLatch(5);

    // 注册从序列0开始的监听器（从头开始接收所有消息）
    UUID listenerId = reliableTopic.addMessageListener(new ReliableMessageListener<String>() {
      private long sequence = 0; // 从头开始

      @Override
      public void onMessage(Message<String> message) {
        System.out.println("重放接收消息: " + message.getMessageObject());
        latch.countDown();
      }

      @Override
      public long retrieveInitialSequence() {
        return 0; // 从头开始接收所有消息
      }

      @Override
      public void storeSequence(long sequence) {
        this.sequence = sequence;
      }

      @Override
      public boolean isLossTolerant() {
        return true;
      }

      @Override
      public boolean isTerminal(Throwable failure) {
        return failure != null;
      }
    });

    try {
      // 等待消息重放完成
      boolean completed = latch.await(10, TimeUnit.SECONDS);
      if (completed) {
        System.out.println("消息重放完成");
      } else {
        System.out.println("消息重放超时");
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }

    // 移除监听器
    reliableTopic.removeMessageListener(listenerId);
    System.out.println("已移除重放监听器");
  }
}