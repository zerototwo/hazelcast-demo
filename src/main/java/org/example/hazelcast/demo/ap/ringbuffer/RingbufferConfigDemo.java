package org.example.hazelcast.demo.ap.ringbuffer;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.RingbufferConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.ringbuffer.Ringbuffer;
import org.springframework.stereotype.Component;

/**
 * Hazelcast Ringbuffer 配置示例
 */
@Component
public class RingbufferConfigDemo {

  private final HazelcastInstance hazelcastInstance;

  public RingbufferConfigDemo(HazelcastInstance hazelcastInstance) {
    this.hazelcastInstance = hazelcastInstance;
  }

  /**
   * 运行配置示例
   */
  public void runAllExamples() {
    System.out.println("=================== Hazelcast Ringbuffer 配置示例 ===================");

    displayDefaultConfiguration();
    programmaticConfiguration();
    inMemoryFormatConfiguration();
    timeToLiveConfiguration();
    capacityPolicyConfiguration();
  }

  /**
   * 显示默认配置
   */
  public void displayDefaultConfiguration() {
    System.out.println("\n--- 默认 Ringbuffer 配置 ---");

    Ringbuffer<String> defaultRingbuffer = hazelcastInstance.getRingbuffer("default-ringbuffer");

    System.out.println("默认 Ringbuffer 容量: " + defaultRingbuffer.capacity());
    System.out.println("Ringbuffer 名称: " + defaultRingbuffer.getName());
    System.out.println("Ringbuffer 分区 ID: " + defaultRingbuffer.getPartitionKey());
    System.out.println("服务名称: " + defaultRingbuffer.getServiceName());

    // 清理测试使用的 Ringbuffer
    try {
      while (defaultRingbuffer.size() > 0) {
        defaultRingbuffer.readOne(defaultRingbuffer.headSequence());
      }
    } catch (Exception e) {
      // 忽略异常
    }
  }

  /**
   * 编程方式配置 Ringbuffer
   */
  public void programmaticConfiguration() {
    System.out.println("\n--- 编程方式配置 Ringbuffer ---");

    // 注意：这里只是演示如何创建配置，实际不会影响已经启动的实例
    Config config = new Config();
    RingbufferConfig rbConfig = new RingbufferConfig("programmatic-rb")
        .setCapacity(5000)
        .setBackupCount(1)
        .setAsyncBackupCount(0)
        .setTimeToLiveSeconds(0)
        .setInMemoryFormat(InMemoryFormat.BINARY);

    System.out.println("编程方式配置的 Ringbuffer 容量: " + rbConfig.getCapacity());
    System.out.println("同步备份数: " + rbConfig.getBackupCount());
    System.out.println("异步备份数: " + rbConfig.getAsyncBackupCount());
    System.out.println("生存时间(秒): " + rbConfig.getTimeToLiveSeconds());
    System.out.println("内存格式: " + rbConfig.getInMemoryFormat());
  }

  /**
   * 内存格式配置
   */
  public void inMemoryFormatConfiguration() {
    System.out.println("\n--- 内存格式配置 ---");

    System.out.println("Hazelcast Ringbuffer 支持两种内存格式:");
    System.out.println("1. BINARY (默认) - 项目以序列化形式存储，提高性能");
    System.out.println("2. OBJECT - 项目以反序列化对象形式存储，适用于过滤和小对象");

    // 注意：这里只是演示如何配置
    Config config = new Config();
    config.getRingbufferConfig("binary-rb")
        .setInMemoryFormat(InMemoryFormat.BINARY);

    config.getRingbufferConfig("object-rb")
        .setInMemoryFormat(InMemoryFormat.OBJECT);

    System.out.println("\n建议使用场景:");
    System.out.println("- 当需要对 Ringbuffer 项目进行过滤时，使用 OBJECT 格式");
    System.out.println("- 当 Ringbuffer 项目很大或需要高性能时，使用 BINARY 格式");
  }

  /**
   * 生存时间配置
   */
  public void timeToLiveConfiguration() {
    System.out.println("\n--- 生存时间配置 ---");

    System.out.println("可以为 Ringbuffer 项目配置生存时间（TTL）:");
    System.out.println("- 默认值为 0，表示项目永不过期");
    System.out.println("- 如果设置为正值（如 3600 秒），项目在该时间后自动过期");

    // 注意：这里只是演示
    Config config = new Config();
    config.getRingbufferConfig("ttl-rb")
        .setTimeToLiveSeconds(3600); // 1小时后过期

    System.out.println("\n使用 TTL 的好处:");
    System.out.println("- 自动清理旧数据，避免内存占用");
    System.out.println("- 实现基于时间的滚动窗口");
    System.out.println("- 适合临时性的事件流处理");
  }

  /**
   * 容量策略配置
   */
  public void capacityPolicyConfiguration() {
    System.out.println("\n--- 容量策略配置 ---");

    System.out.println("使用 Ringbuffer 时，需要考虑以下容量相关配置:");
    System.out.println("1. 容量设置 - 确定 Ringbuffer 可以存储的最大项目数");
    System.out.println("2. 溢出策略 - 决定当 Ringbuffer 已满时的行为:");
    System.out.println("   - OVERWRITE: 覆盖最旧的项目");
    System.out.println("   - FAIL: 拒绝添加并返回失败");

    // 注意：这里只是演示
    Config config = new Config();
    config.getRingbufferConfig("large-capacity-rb")
        .setCapacity(100000); // 10万个项目

    System.out.println("\n容量选择考虑因素:");
    System.out.println("- 内存使用量：较大的容量需要更多内存");
    System.out.println("- 项目大小：存储大对象需要更多内存");
    System.out.println("- 使用场景：事件流、日志等使用较大容量");
    System.out.println("- 生产者/消费者速率：生产快于消费时需要更大容量");
  }
}