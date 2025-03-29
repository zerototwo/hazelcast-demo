package org.example.hazelcast.demo.events;

import com.hazelcast.collection.ISet;
import com.hazelcast.collection.ItemEvent;
import com.hazelcast.collection.ItemListener;
import com.hazelcast.cluster.Member;
import com.hazelcast.map.IMap;
import com.hazelcast.map.listener.EntryAddedListener;
import com.hazelcast.map.listener.EntryRemovedListener;
import com.hazelcast.map.listener.EntryUpdatedListener;
import com.hazelcast.topic.ITopic;
import com.hazelcast.topic.Message;
import com.hazelcast.topic.MessageListener;
import com.hazelcast.core.DistributedObjectEvent;
import com.hazelcast.core.DistributedObjectListener;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.LifecycleEvent;
import com.hazelcast.core.LifecycleListener;
import com.hazelcast.cluster.MembershipListener;
import com.hazelcast.cluster.MembershipEvent;
import org.springframework.stereotype.Component;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * 演示Hazelcast分布式事件系统的示例类
 */
@Component
public class EventsDemo {
  private final HazelcastInstance hazelcastInstance;
  private UUID registeredMapListenerId;
  private UUID registeredSetListenerId;
  private UUID registeredTopicListenerId;
  private UUID registeredLifecycleListenerId;
  private UUID registeredMembershipListenerId;
  private UUID registeredDistributedObjectListenerId;

  public EventsDemo(HazelcastInstance hazelcastInstance) {
    this.hazelcastInstance = hazelcastInstance;
  }

  /**
   * 运行所有事件演示
   */
  public void runAllDemos() {
    System.out.println("============== Hazelcast事件演示 ==============");

    demoMapEntryListener();
    demoCollectionItemListener();
    demoTopicMessageListener();
    demoLifecycleListener();
    demoMembershipListener();
    demoDistributedObjectListener();

    System.out.println("\n所有演示完成，清理资源...");
    removeAllListeners();
  }

  /**
   * 演示Map条目监听器
   */
  public void demoMapEntryListener() {
    System.out.println("\n--- 演示Map条目监听器 ---");

    IMap<String, String> map = hazelcastInstance.getMap("demo-map");
    map.clear(); // 确保Map是空的

    // 创建并注册监听器
    MapEntryListener entryListener = new MapEntryListener();
    registeredMapListenerId = map.addEntryListener(entryListener, true);
    System.out.println("Map条目监听器已注册，ID: " + registeredMapListenerId);

    // 执行操作触发事件
    System.out.println("\n执行Map操作以触发事件...");
    map.put("key1", "value1");
    map.put("key2", "value2");
    map.put("key1", "updated-value1");
    map.remove("key2");

    // 等待事件处理
    sleep(500);
  }

  /**
   * 演示集合项目监听器
   */
  public void demoCollectionItemListener() {
    System.out.println("\n--- 演示集合项目监听器 ---");

    ISet<String> set = hazelcastInstance.getSet("demo-set");
    set.clear(); // 确保集合是空的

    // 创建并注册监听器
    CollectionItemListener itemListener = new CollectionItemListener();
    registeredSetListenerId = set.addItemListener(itemListener, true);
    System.out.println("集合项目监听器已注册，ID: " + registeredSetListenerId);

    // 执行操作触发事件
    System.out.println("\n执行集合操作以触发事件...");
    set.add("item1");
    set.add("item2");
    set.remove("item1");

    // 等待事件处理
    sleep(500);
  }

  /**
   * 演示Topic消息监听器
   */
  public void demoTopicMessageListener() {
    System.out.println("\n--- 演示Topic消息监听器 ---");

    ITopic<String> topic = hazelcastInstance.getTopic("demo-topic");

    // 创建并注册监听器
    TopicMessageListener messageListener = new TopicMessageListener();
    registeredTopicListenerId = topic.addMessageListener(messageListener);
    System.out.println("Topic消息监听器已注册，ID: " + registeredTopicListenerId);

    // 发布消息触发事件
    System.out.println("\n发布消息以触发事件...");
    topic.publish("这是一条测试消息");
    topic.publish("这是另一条测试消息");

    // 等待事件处理
    sleep(500);
  }

  /**
   * 演示生命周期监听器
   */
  public void demoLifecycleListener() {
    System.out.println("\n--- 演示生命周期监听器 ---");

    // 创建并注册监听器
    InstanceLifecycleListener lifecycleListener = new InstanceLifecycleListener();
    registeredLifecycleListenerId = hazelcastInstance.getLifecycleService().addLifecycleListener(lifecycleListener);
    System.out.println("生命周期监听器已注册，ID: " + registeredLifecycleListenerId);

    System.out.println("注意：生命周期事件只会在Hazelcast实例状态变化时触发");
    System.out.println("例如：启动、关闭、合并等状态");
  }

  /**
   * 演示成员关系监听器
   */
  public void demoMembershipListener() {
    System.out.println("\n--- 演示成员关系监听器 ---");

    // 创建并注册监听器
    ClusterMembershipListener membershipListener = new ClusterMembershipListener();
    registeredMembershipListenerId = hazelcastInstance.getCluster().addMembershipListener(membershipListener);
    System.out.println("成员关系监听器已注册，ID: " + registeredMembershipListenerId);

    System.out.println("注意：成员关系事件只会在集群成员加入、离开或属性变化时触发");
  }

  /**
   * 演示分布式对象监听器
   */
  public void demoDistributedObjectListener() {
    System.out.println("\n--- 演示分布式对象监听器 ---");

    // 创建并注册监听器
    DistributedObjectsListener distributedObjectListener = new DistributedObjectsListener();
    registeredDistributedObjectListenerId = hazelcastInstance.addDistributedObjectListener(distributedObjectListener);
    System.out.println("分布式对象监听器已注册，ID: " + registeredDistributedObjectListenerId);

    // 创建和销毁分布式对象触发事件
    System.out.println("\n创建和销毁分布式对象以触发事件...");
    IMap<String, String> tempMap = hazelcastInstance.getMap("temp-map-" + UUID.randomUUID());
    tempMap.destroy();

    // 等待事件处理
    sleep(500);
  }

  /**
   * 移除所有注册的监听器
   */
  private void removeAllListeners() {
    if (registeredMapListenerId != null) {
      hazelcastInstance.getMap("demo-map").removeEntryListener(registeredMapListenerId);
      System.out.println("已移除Map条目监听器");
    }

    if (registeredSetListenerId != null) {
      hazelcastInstance.getSet("demo-set").removeItemListener(registeredSetListenerId);
      System.out.println("已移除集合项目监听器");
    }

    if (registeredTopicListenerId != null) {
      hazelcastInstance.getTopic("demo-topic").removeMessageListener(registeredTopicListenerId);
      System.out.println("已移除Topic消息监听器");
    }

    if (registeredLifecycleListenerId != null) {
      hazelcastInstance.getLifecycleService().removeLifecycleListener(registeredLifecycleListenerId);
      System.out.println("已移除生命周期监听器");
    }

    if (registeredMembershipListenerId != null) {
      hazelcastInstance.getCluster().removeMembershipListener(registeredMembershipListenerId);
      System.out.println("已移除成员关系监听器");
    }

    if (registeredDistributedObjectListenerId != null) {
      hazelcastInstance.removeDistributedObjectListener(registeredDistributedObjectListenerId);
      System.out.println("已移除分布式对象监听器");
    }
  }

  /**
   * 辅助方法：暂停执行一段时间
   */
  private void sleep(long milliseconds) {
    try {
      TimeUnit.MILLISECONDS.sleep(milliseconds);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  /**
   * Map条目监听器实现
   */
  private static class MapEntryListener implements EntryAddedListener<String, String>,
      EntryRemovedListener<String, String>,
      EntryUpdatedListener<String, String> {
    @Override
    public void entryAdded(EntryEvent<String, String> event) {
      System.out.println("MAP事件 - 添加: " + event.getKey() + " -> " + event.getValue());
    }

    @Override
    public void entryRemoved(EntryEvent<String, String> event) {
      System.out.println("MAP事件 - 删除: " + event.getKey());
    }

    @Override
    public void entryUpdated(EntryEvent<String, String> event) {
      System.out.println("MAP事件 - 更新: " + event.getKey() + " -> " + event.getValue() +
          " (旧值: " + event.getOldValue() + ")");
    }
  }

  /**
   * 集合项目监听器实现
   */
  private static class CollectionItemListener implements ItemListener<String> {
    @Override
    public void itemAdded(ItemEvent<String> event) {
      System.out.println("集合事件 - 添加项目: " + event.getItem());
    }

    @Override
    public void itemRemoved(ItemEvent<String> event) {
      System.out.println("集合事件 - 删除项目: " + event.getItem());
    }
  }

  /**
   * Topic消息监听器实现
   */
  private static class TopicMessageListener implements MessageListener<String> {
    @Override
    public void onMessage(Message<String> message) {
      System.out.println("TOPIC事件 - 收到消息: " + message.getMessageObject() +
          " (发布时间: " + message.getPublishTime() + ")");
    }
  }

  /**
   * 生命周期监听器实现
   */
  private static class InstanceLifecycleListener implements LifecycleListener {
    @Override
    public void stateChanged(LifecycleEvent event) {
      System.out.println("生命周期事件 - 状态变化: " + event.getState());
    }
  }

  /**
   * 成员关系监听器实现
   */
  private static class ClusterMembershipListener implements MembershipListener {
    @Override
    public void memberAdded(MembershipEvent membershipEvent) {
      System.out.println("成员关系事件 - 成员加入: " + membershipEvent.getMember());
    }

    @Override
    public void memberRemoved(MembershipEvent membershipEvent) {
      System.out.println("成员关系事件 - 成员离开: " + membershipEvent.getMember());
    }
  }

  /**
   * 分布式对象监听器实现
   */
  private static class DistributedObjectsListener implements DistributedObjectListener {
    @Override
    public void distributedObjectCreated(DistributedObjectEvent event) {
      System.out.println("分布式对象事件 - 对象创建: " + event.getDistributedObject().getName() +
          " (类型: " + event.getDistributedObject().getServiceName() + ")");
    }

    @Override
    public void distributedObjectDestroyed(DistributedObjectEvent event) {
      System.out.println("分布式对象事件 - 对象销毁: " + event.getObjectName() +
          " (类型: " + event.getServiceName() + ")");
    }
  }
}