package org.example.hazelcast.demo.datastructure.ap.flakeid;

import com.hazelcast.config.Config;
import com.hazelcast.config.FlakeIdGeneratorConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.flakeidgen.FlakeIdGenerator;
import org.springframework.stereotype.Component;

/**
 * Hazelcast Flake ID Generator 配置示例
 * 
 * 此类展示了Flake ID Generator的各种配置选项和最佳实践。
 * 
 * Flake ID Generator提供多种配置参数来调整其行为，包括：
 * - 预取机制(prefetch)：提高性能的关键，通过批量预先获取ID来减少网络调用
 * - 节点ID偏移(node-id-offset)：用于多集群部署场景，确保不同集群生成的ID不会冲突
 * - 历元开始时间(epoch-start)：ID时间戳部分的起始点，默认为2018年1月1日
 * - 位长度配置：调整序列号和节点ID在最终ID中所占的位数
 * - 统计(statistics)：控制是否收集性能和使用统计信息
 * - 允许的未来时间：控制在高负载下，ID生成可以提前多少时间
 * 
 * 正确的配置对于性能和唯一性至关重要，应根据具体应用场景进行调整。
 */
@Component
public class FlakeIdConfigDemo {

  private final HazelcastInstance hazelcastInstance;

  public FlakeIdConfigDemo(HazelcastInstance hazelcastInstance) {
    this.hazelcastInstance = hazelcastInstance;
  }

  /**
   * 运行所有配置示例
   * 
   * 按顺序展示Flake ID Generator的各种配置选项及其影响：
   * 1. 默认配置值
   * 2. 编程式配置方法
   * 3. 预取机制配置
   * 4. 节点ID偏移量配置
   * 5. 历元开始时间配置
   * 6. 位长度配置
   * 7. 统计配置
   * 8. 允许的未来时间配置
   */
  public void runAllExamples() {
    System.out.println("=================== Hazelcast Flake ID Generator 配置示例 ===================");

    defaultConfigurationExample();
    programmaticConfigExample();
    prefetchConfigExample();
    nodeIdOffsetExample();
    epochStartExample();
    bitLengthConfigExample();
    statisticsConfigExample();
    allowedFutureMillisExample();
  }

  /**
   * 默认配置示例
   * 
   * 展示Flake ID Generator的默认配置值，这些值对大多数应用场景都适用，
   * 但在特定需求下可能需要调整。
   */
  public void defaultConfigurationExample() {
    System.out.println("\n--- 默认配置示例 ---");

    // 获取默认配置的FlakeIdGenerator实例
    FlakeIdGenerator idGenerator = hazelcastInstance.getFlakeIdGenerator("default-config-generator");

    // 展示默认配置参数
    System.out.println("Flake ID Generator 默认配置值:");
    System.out.println("- prefetch-count: 100 (预取ID数量)");
    System.out.println("- prefetch-validity-millis: 600000 (预取ID有效期，10分钟)");
    System.out.println("- epoch-start: 1514764800000 (2018年1月1日 00:00:00 UTC)");
    System.out.println("- node-id-offset: 0 (节点ID偏移量)");
    System.out.println("- bits-sequence: 6 (序列号位数)");
    System.out.println("- bits-node-id: 16 (节点ID位数)");
    System.out.println("- allowed-future-millis: 15000 (允许的未来时间毫秒数，15秒)");
    System.out.println("- statistics-enabled: true (是否启用统计)");

    // 使用默认配置生成ID示例
    long id = idGenerator.newId();
    System.out.println("\n使用默认配置生成的ID: " + id);
  }

  /**
   * 编程式配置示例
   * 
   * 演示如何通过代码方式配置Flake ID Generator的各种参数。
   * 这种方式适合在应用启动时动态设置配置。
   * 
   * 注意：此示例中的配置不会影响已运行的Hazelcast实例，仅用于演示。
   * 实际应用中，这些配置应在Hazelcast实例创建前设置。
   */
  public void programmaticConfigExample() {
    System.out.println("\n--- 编程式配置示例 ---");

    // 创建Hazelcast配置对象
    Config config = new Config();

    // 创建并配置FlakeIdGenerator配置
    FlakeIdGeneratorConfig genConfig = new FlakeIdGeneratorConfig("programmatic-config-generator");
    genConfig.setPrefetchCount(200) // 设置预取数量为200
        .setPrefetchValidityMillis(30000) // 设置预取有效期为30秒
        .setStatisticsEnabled(true); // 启用统计收集

    // 将FlakeIdGenerator配置添加到Hazelcast配置中
    config.addFlakeIdGeneratorConfig(genConfig);

    // 展示配置结果
    System.out.println("通过编程方式配置Flake ID Generator:");
    System.out.println("- 名称: " + genConfig.getName());
    System.out.println("- 预取数量: " + genConfig.getPrefetchCount());
    System.out.println("- 预取有效期(毫秒): " + genConfig.getPrefetchValidityMillis());
    System.out.println("- 统计启用: " + genConfig.isStatisticsEnabled());
  }

  /**
   * 预取配置示例
   * 
   * 详细说明预取机制对Flake ID Generator性能的影响。
   * 预取机制是提高性能的关键，通过一次网络请求获取多个ID，
   * 减少网络通信次数。
   */
  public void prefetchConfigExample() {
    System.out.println("\n--- 预取配置示例 ---");

    // 解释预取数量参数
    System.out.println("预取机制是Flake ID Generator性能优化的关键:");
    System.out.println("1. prefetch-count: 一次调用预取的ID数量");
    System.out.println("   - 范围: 1 到 100,000");
    System.out.println("   - 默认值: 100");
    System.out.println("   - 小值: 减少内存占用，但可能增加网络调用");
    System.out.println("   - 大值: 提高性能，但增加内存使用");

    // 解释预取有效期参数
    System.out.println("\n2. prefetch-validity-millis: 预取ID的有效期");
    System.out.println("   - 默认值: 600,000 (10分钟)");
    System.out.println("   - 值为0: 无限期有效，但ID可能不是粗略有序的");
    System.out.println("   - 较大值: 减少网络调用，但ID顺序性降低");
    System.out.println("   - 较小值: 提高ID顺序性，但增加网络调用");

    // 解释预取机制工作原理
    System.out.println("\n预取机制的工作原理:");
    System.out.println("- 客户端首次调用newId()时会从成员节点批量获取多个ID");
    System.out.println("- 之后的调用使用本地缓存的ID，无需网络请求");
    System.out.println("- 当缓存ID用尽或过期后，会自动执行新一轮预取");
  }

  /**
   * 节点ID偏移量示例
   * 
   * 说明节点ID偏移量的用途，特别是在多集群部署场景中如何确保
   * 不同集群生成的ID不会冲突。
   */
  public void nodeIdOffsetExample() {
    System.out.println("\n--- 节点ID偏移量示例 ---");

    // 解释节点ID偏移量的用途
    System.out.println("节点ID偏移量(node-id-offset)的用途:");
    System.out.println("- 为集群中的节点分配唯一标识符时添加偏移量");
    System.out.println("- 默认值为0，表示不添加偏移量");
    System.out.println("- 主要用于A/B部署场景，确保不同集群生成的ID不冲突");

    // 举例说明使用场景
    System.out.println("\n使用场景示例:");
    System.out.println("假设有两个Hazelcast集群A和B，您希望同时运行它们并生成唯一ID:");
    System.out.println("1. 集群A使用默认配置 (node-id-offset = 0)");
    System.out.println("2. 集群B配置为 node-id-offset = 1000");
    System.out.println("这样，即使两个集群的节点加入顺序相同，生成的ID也不会冲突");
  }

  /**
   * 历元开始时间示例
   * 
   * 解释历元开始时间(epoch-start)参数的含义及其对ID生成的影响。
   * 这个参数决定了ID中时间戳部分的起始点。
   */
  public void epochStartExample() {
    System.out.println("\n--- 历元开始时间示例 ---");

    // 解释历元开始时间的含义
    System.out.println("历元开始时间(epoch-start)的含义:");
    System.out.println("- 决定Flake ID的时间戳部分计算的起始点");
    System.out.println("- 默认值为1514764800000 (2018年1月1日 00:00:00 UTC)");
    System.out.println("- 时间戳部分占41位，提供约70年的可用期限(到2088年)");

    // 解释自定义历元开始时间的考虑因素
    System.out.println("\n自定义历元开始时间的考虑因素:");
    System.out.println("- 如果您的应用需要更长的有效期，可以调整为更晚的日期");
    System.out.println("- 如果您需要生成的ID具有特定的时间语义，可以调整为特定日期");
    System.out.println("- 注意：更改此值会导致不同配置下生成的ID不兼容");
  }

  /**
   * 位长度配置示例
   * 
   * 详细解释ID中各部分的位长度配置及其影响。
   * Flake ID由三个部分组成，每部分的位长度可以配置，但总和必须小于等于63位。
   */
  public void bitLengthConfigExample() {
    System.out.println("\n--- 位长度配置示例 ---");

    // 解释ID组成和位长度配置
    System.out.println("Flake ID 由三个部分组成，每部分的位长度可配置:");

    // 序列号部分
    System.out.println("1. bits-sequence: 序列号部分的位数");
    System.out.println("   - 默认值: 6位");
    System.out.println("   - 含义: 每毫秒最多可生成2^6=64个ID");
    System.out.println("   - 增加此值可提高单毫秒内的ID生成数量，但会减少其他部分的位数");

    // 节点ID部分
    System.out.println("\n2. bits-node-id: 节点ID部分的位数");
    System.out.println("   - 默认值: 16位");
    System.out.println("   - 含义: 最多支持2^16=65536个不同的节点ID");
    System.out.println("   - 对于大型集群，确保此值足够大");
    System.out.println("   - 节点ID溢出处理: 当集群成员加入版本超过2^16时，ID生成会转发到其他成员");

    // 时间戳部分
    System.out.println("\n3. 时间戳部分(固定41位)");
    System.out.println("   - 包含自epoch-start以来的毫秒数");
    System.out.println("   - 提供约70年的可用期限");
    System.out.println("   - 注意: 三个部分的位数总和必须小于等于63 (因为最终ID为long类型)");
  }

  /**
   * 统计配置示例
   * 
   * 解释统计配置的作用及如何获取统计信息。
   * 开启统计可以帮助监控ID生成性能，但会略微增加开销。
   */
  public void statisticsConfigExample() {
    System.out.println("\n--- 统计配置示例 ---");

    // 解释统计配置的作用
    System.out.println("statistics-enabled配置决定是否收集Flake ID Generator的统计信息:");
    System.out.println("- 默认值为true，表示启用统计收集");
    System.out.println("- 设置为false可以稍微提高性能，但会失去监控能力");

    // 说明如何获取统计信息
    System.out.println("\n统计信息可通过以下方式获取:");
    System.out.println("1. 在代码中使用getLocalFlakeIdGeneratorStats()方法");
    System.out.println("2. 通过Hazelcast Management Center进行可视化监控");

    // 列出收集的统计数据类型
    System.out.println("\n收集的统计数据包括:");
    System.out.println("- ID生成次数");
    System.out.println("- 预取批次数量");
    System.out.println("- 预取操作的延迟");
  }

  /**
   * 允许的未来时间示例
   * 
   * 解释allowed-future-millis配置的作用及其在高负载场景下的重要性。
   * 此参数控制在高请求率下，ID生成可以提前多少时间，以避免过度阻塞。
   */
  public void allowedFutureMillisExample() {
    System.out.println("\n--- 允许的未来时间示例 ---");

    // 解释allowed-future-millis配置的作用
    System.out.println("allowed-future-millis配置的作用:");
    System.out.println("- 控制生成器允许超前于当前时间的最大毫秒数");
    System.out.println("- 默认值为15000 (15秒)");
    System.out.println("- 如果请求速率过高，导致序列号溢出到未来时间，生成器会等待");
    System.out.println("- 但允许最多提前15秒，以应对高峰请求");

    // 解释此设置的意义
    System.out.println("\n此设置的意义:");
    System.out.println("- 防止在极高请求速率下生成过多未来时间的ID，保持ID的时间语义");
    System.out.println("- 保护系统不会无限期地前进时间，导致大量未使用的序列号空间");
    System.out.println("- 在达到限制时，操作会阻塞等待时间追上来，而不是失败");
  }
}