# Hazelcast分布式事件指南

## 概述

Hazelcast提供了一个强大的事件机制，允许您监听集群中发生的各种事件。当您在集群的一个成员上注册事件监听器时，该监听器实际上会监听来自集群中任何成员的事件。当新成员加入集群时，来自新成员的事件也会被传递给监听器。

需要注意的是，只有在注册了事件监听器的情况下才会创建事件。如果没有注册监听器，则不会创建事件。如果在注册事件监听器时提供了谓词（predicate），则在将事件发送给监听器之前会对谓词进行验证。

作为经验法则，您的事件监听器不应在其事件方法中实现长时间阻塞线程的重处理。如果需要，您可以使用`ExecutorService`将长时间运行的处理转移到另一个线程，从而减轻当前监听器线程的负担。

> **注意**：在故障转移场景中，事件不是高可用的，可能会丢失。但是，您可以通过配置事件队列容量等方式来解决这个问题。

## Hazelcast事件监听器类型

Hazelcast提供了多种类型的事件监听器，主要分为以下几类：

### 集群事件

- **Membership Listener** - 监听集群成员变化事件
- **Distributed Object Listener** - 监听分布式对象的创建和销毁事件
- **Migration Listener** - 监听分区迁移的开始和完成事件
- **Partition Lost Listener** - 监听分区丢失事件
- **Lifecycle Listener** - 监听`HazelcastInstance`生命周期事件
- **Client Listener** - 监听客户端连接事件

### 分布式对象事件

- **Entry Listener** - 监听`IMap`和`MultiMap`的条目事件
- **Item Listener** - 监听`IQueue`、`ISet`和`IList`的项目事件
- **Message Listener** - 监听`ITopic`的消息事件
- **Reliable Message Listener** - 监听`ReliableTopic`的消息事件

### JCache事件

- **Cache Entry Listener** - 监听JCache条目变化
- **ICache Partition Lost Listener** - 监听ICache分区丢失

### 客户端事件

- **Lifecycle Listener** - 监听客户端生命周期事件
- **Membership Listener** - 监听集群成员变化事件（客户端视角）
- **Distributed Object Listener** - 监听分布式对象变化（客户端视角）

### 作业监控事件

- **Job Status Listener** - 监听Hazelcast作业状态变化

## 实现事件监听器

### 1. Entry Listener（条目监听器）

IMap和MultiMap的Entry Listener是最常用的监听器之一，允许您监听Map条目的各种事件：

- **entryAdded** - 添加条目时
- **entryRemoved** - 删除条目时
- **entryUpdated** - 更新条目时
- **entryEvicted** - 驱逐条目时
- **entryExpired** - 条目过期时
- **mapEvicted** - 清除整个映射时
- **mapCleared** - 清除映射时

示例代码：

```java
public class MyEntryListener implements EntryAddedListener<String, String>, 
                                        EntryRemovedListener<String, String>,
                                        EntryUpdatedListener<String, String> {
    @Override
    public void entryAdded(EntryEvent<String, String> event) {
        System.out.println("添加了条目：" + event.getKey() + " -> " + event.getValue());
    }

    @Override
    public void entryRemoved(EntryEvent<String, String> event) {
        System.out.println("删除了条目：" + event.getKey());
    }

    @Override
    public void entryUpdated(EntryEvent<String, String> event) {
        System.out.println("更新了条目：" + event.getKey() + " -> " + event.getValue());
    }
}

// 注册监听器
map.addEntryListener(new MyEntryListener(), true);

// 使用谓词过滤事件
Predicate<String, String> predicate = Predicates.sql("this == 'desired-value'");
map.addEntryListener(new MyEntryListener(), predicate, true);
```

### 2. Membership Listener（成员关系监听器）

成员关系监听器允许您监听集群成员的加入、离开和属性变化：

```java
public class ClusterMembershipListener implements MembershipListener {
    @Override
    public void memberAdded(MembershipEvent membershipEvent) {
        System.out.println("新成员加入：" + membershipEvent.getMember());
    }

    @Override
    public void memberRemoved(MembershipEvent membershipEvent) {
        System.out.println("成员离开：" + membershipEvent.getMember());
    }

    @Override
    public void memberAttributeChanged(MemberAttributeEvent memberAttributeEvent) {
        System.out.println("成员属性变化：" + memberAttributeEvent.getMember());
    }
}

// 注册监听器
hazelcastInstance.getCluster().addMembershipListener(new ClusterMembershipListener());
```

### 3. Item Listener（项目监听器）

IQueue, ISet和IList的Item Listener允许您监听集合项目的添加和移除：

```java
public class CollectionItemListener implements ItemListener<String> {
    @Override
    public void itemAdded(ItemEvent<String> itemEvent) {
        System.out.println("添加了项目：" + itemEvent.getItem());
    }

    @Override
    public void itemRemoved(ItemEvent<String> itemEvent) {
        System.out.println("移除了项目：" + itemEvent.getItem());
    }
}

// 注册监听器
ISet<String> set = hazelcastInstance.getSet("my-set");
set.addItemListener(new CollectionItemListener(), true);
```

### 4. Message Listener（消息监听器）

ITopic的Message Listener允许您监听发布到主题的消息：

```java
public class TopicMessageListener implements MessageListener<String> {
    @Override
    public void onMessage(Message<String> message) {
        System.out.println("收到消息：" + message.getMessageObject());
    }
}

// 注册监听器
ITopic<String> topic = hazelcastInstance.getTopic("my-topic");
topic.addMessageListener(new TopicMessageListener());
```

### 5. Lifecycle Listener（生命周期监听器）

生命周期监听器允许您监听Hazelcast实例的生命周期事件：

```java
public class InstanceLifecycleListener implements LifecycleListener {
    @Override
    public void stateChanged(LifecycleEvent event) {
        System.out.println("生命周期状态变化：" + event.getState());
    }
}

// 注册监听器
hazelcastInstance.getLifecycleService().addLifecycleListener(new InstanceLifecycleListener());
```

## 全局事件配置

Hazelcast允许您配置事件系统的一些全局参数，如下所示：

```java
Config config = new Config();
config.setProperty("hazelcast.event.queue.capacity", "2000000");
config.setProperty("hazelcast.event.queue.timeout.millis", "500");
config.setProperty("hazelcast.event.thread.count", "5");
```

这些参数分别用于：

- `hazelcast.event.queue.capacity`：事件队列的容量（默认为1,000,000）
- `hazelcast.event.queue.timeout.millis`：将事件放入事件队列的超时时间（默认为250毫秒）
- `hazelcast.event.thread.count`：事件线程数量（默认为5）

## 最佳实践

1. **轻量级监听器**：确保事件监听器中的处理逻辑轻量级，避免长时间阻塞线程。对于耗时操作，将处理逻辑分派到单独的线程或线程池。

2. **适当的谓词使用**：在注册监听器时使用谓词来过滤事件，减少不必要的事件处理。

3. **容错性设计**：在故障转移场景中事件可能会丢失，确保您的应用程序能够处理这种情况。

4. **配置事件队列**：对于高吞吐量系统，考虑增加事件队列容量和超时设置。

5. **本地与远程事件**：对于频繁发生的事件，考虑只监听本地事件（设置`localOnly=true`）以减少网络开销。

## 结论

Hazelcast分布式事件系统提供了一种强大的机制来监听和响应集群中的各种变化。通过合理地使用事件监听器，您可以构建响应式的分布式应用程序，能够实时地对集群状态变化和数据操作做出反应。

## 参考资料

- [Hazelcast事件文档](https://docs.hazelcast.com/hazelcast/latest/events/distributed-events)
- [全局事件配置](https://docs.hazelcast.com/hazelcast/latest/events/global-event-configuration) 