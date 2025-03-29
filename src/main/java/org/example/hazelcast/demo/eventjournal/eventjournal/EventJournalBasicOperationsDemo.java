package org.example.hazelcast.demo.eventjournal.eventjournal;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.map.MapEvent;
import com.hazelcast.map.listener.EntryAddedListener;
import com.hazelcast.map.listener.EntryRemovedListener;
import com.hazelcast.map.listener.EntryUpdatedListener;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryEventType;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.function.Predicate;
import java.util.function.Function;

/**
 * Event Journal基本操作示例
 * <p>
 * Event Journal是一个分布式数据结构，用于存储map或cache上的变更历史记录。
 * 每个修改数据结构内容的操作（如put、remove或者由自动过期/驱逐引起的变更）
 * 都会创建一个事件并存储在Event Journal中。
 * </p>
 * 
 * <p>
 * <strong>Event Journal的主要特性：</strong>
 * </p>
 * <ul>
 * <li><strong>历史记录：</strong> 存储数据结构的所有修改操作</li>
 * <li><strong>事件详情：</strong> 包含事件类型、键、旧值和新值（如适用）</li>
 * <li><strong>固定容量：</strong> 可配置的容量限制（默认10000）</li>
 * <li><strong>过期机制：</strong> 支持设置time-to-live</li>
 * <li><strong>分区存储：</strong> 按事件键分区，与相关Map/Cache条目共存</li>
 * </ul>
 * 
 * <p>
 * <strong>适用场景：</strong>
 * </p>
 * <ul>
 * <li>数据流处理</li>
 * <li>变更数据捕获（CDC）</li>
 * <li>审计日志</li>
 * <li>数据复制</li>
 * <li>状态重建</li>
 * </ul>
 * 
 * <p>
 * <strong>使用注意事项：</strong>
 * </p>
 * <ul>
 * <li>Event Journal只能在数据流水线中使用</li>
 * <li>与驱逐和过期配置交互可能导致不同分区副本的事件不一致</li>
 * <li>读取操作需注意处理异步特性</li>
 * </ul>
 * 
 * <p>
 * 注意：此示例代码主要用于演示目的，使用了模拟实现。
 * 实际Event Journal的完整功能需要使用Hazelcast Enterprise版本
 * </p>
 */
@Component
public class EventJournalBasicOperationsDemo {

  private final HazelcastInstance hazelcastInstance;
  private static final String MAP_NAME = "eventJournalMap";
  private final Random random = new Random();

  // 模拟事件类型
  public enum EventType {
    ADDED, UPDATED, REMOVED, EVICTED, EXPIRED
  }

  // 模拟Map事件
  public static class MapEventItem<K, V> {
    private final EventType type;
    private final K key;
    private final V oldValue;
    private final V newValue;
    private final long sequence;

    public MapEventItem(EventType type, K key, V oldValue, V newValue, long sequence) {
      this.type = type;
      this.key = key;
      this.oldValue = oldValue;
      this.newValue = newValue;
      this.sequence = sequence;
    }

    public EventType getType() {
      return type;
    }

    public K getKey() {
      return key;
    }

    public V getOldValue() {
      return oldValue;
    }

    public V getNewValue() {
      return newValue;
    }

    public long getSequence() {
      return sequence;
    }
  }

  // 用于模拟Event Journal的事件列表
  private final List<MapEventItem<String, String>> eventJournal = new ArrayList<>();
  private long nextSequence = 0;

  /**
   * 构造函数
   * 
   * @param hazelcastInstance Hazelcast实例
   */
  public EventJournalBasicOperationsDemo(HazelcastInstance hazelcastInstance) {
    this.hazelcastInstance = hazelcastInstance;
  }

  /**
   * 运行所有Event Journal示例
   */
  public void runAllExamples() {
    System.out.println("\n==== Event Journal基本操作示例 ====");
    setupEventJournalMap();
    basicReadFromEventJournal();
    filteringEventsExample();
    projectionExample();
    readingFromSequenceExample();
    liveEventMonitoringExample();
    System.out.println("==== Event Journal示例结束 ====\n");
  }

  /**
   * 设置并填充Event Journal Map
   * 
   * <p>
   * 创建一个Map并执行一系列操作，这些操作将被记录到Event Journal中。
   * 操作包括添加、更新和删除条目。
   * </p>
   */
  public void setupEventJournalMap() {
    System.out.println("\n-- 设置Event Journal Map --");

    // 获取或创建Map
    IMap<String, String> map = hazelcastInstance.getMap(MAP_NAME);

    // 清除任何现有数据和事件
    map.clear();
    eventJournal.clear();
    nextSequence = 0;
    System.out.println("已清除Map和事件日志");

    // 添加Map监听器，用于捕获事件
    map.addEntryListener((EntryAddedListener<String, String>) event -> {
      eventJournal.add(new MapEventItem<>(
          EventType.ADDED,
          event.getKey(),
          null,
          event.getValue(),
          nextSequence++));
    }, true);

    map.addEntryListener((EntryUpdatedListener<String, String>) event -> {
      eventJournal.add(new MapEventItem<>(
          EventType.UPDATED,
          event.getKey(),
          event.getOldValue(),
          event.getValue(),
          nextSequence++));
    }, true);

    map.addEntryListener((EntryRemovedListener<String, String>) event -> {
      eventJournal.add(new MapEventItem<>(
          EventType.REMOVED,
          event.getKey(),
          event.getOldValue(),
          null,
          nextSequence++));
    }, true);

    // 添加项目（生成ADDED事件）
    for (int i = 1; i <= 10; i++) {
      map.put("key" + i, "初始值" + i);
    }
    System.out.println("已添加10个初始项目");

    // 更新一些项目（生成UPDATED事件）
    for (int i = 1; i <= 5; i++) {
      map.put("key" + i, "更新值" + i);
    }
    System.out.println("已更新5个项目");

    // 删除一些项目（生成REMOVED事件）
    for (int i = 8; i <= 10; i++) {
      map.remove("key" + i);
    }
    System.out.println("已删除3个项目");

    // 查看当前Map内容
    System.out.println("\n当前Map内容:");
    for (Map.Entry<String, String> entry : map.entrySet()) {
      System.out.println(entry.getKey() + " = " + entry.getValue());
    }
  }

  /**
   * 基本的Event Journal读取示例
   * 
   * <p>
   * 展示如何从Event Journal开始读取事件，并处理这些事件。
   * 这个示例读取所有类型的事件并显示它们的详情。
   * </p>
   */
  public void basicReadFromEventJournal() {
    System.out.println("\n-- 基本的Event Journal读取 --");

    if (eventJournal.isEmpty()) {
      System.out.println("事件日志为空，请先运行setupEventJournalMap()");
      return;
    }

    long oldestSequence = eventJournal.get(0).getSequence();
    long newestSequence = eventJournal.get(eventJournal.size() - 1).getSequence();

    System.out.println("Journal的最老序列号: " + oldestSequence);
    System.out.println("Journal的最新序列号: " + newestSequence);

    // 创建一个结果集
    ReadResultSet<MapEventItem<String, String>> events = readFromJournal(oldestSequence, 100, null, null);

    // 显示读取的事件
    System.out.println("\n读取的事件 (总计 " + events.size() + "):");
    for (MapEventItem<String, String> event : events) {
      StringBuilder sb = new StringBuilder();
      sb.append("事件类型: ").append(event.getType())
          .append(", 键: ").append(event.getKey());

      // 根据事件类型显示旧值和新值
      switch (event.getType()) {
        case ADDED:
          sb.append(", 新值: ").append(event.getNewValue());
          break;
        case UPDATED:
          sb.append(", 旧值: ").append(event.getOldValue())
              .append(", 新值: ").append(event.getNewValue());
          break;
        case REMOVED:
          sb.append(", 旧值: ").append(event.getOldValue());
          break;
        case EVICTED:
          sb.append(", 被驱逐值: ").append(event.getOldValue());
          break;
        case EXPIRED:
          sb.append(", 已过期值: ").append(event.getOldValue());
          break;
      }

      System.out.println(sb.toString());
    }
  }

  /**
   * 事件过滤示例
   * 
   * <p>
   * 演示如何使用过滤器从Event Journal中仅读取特定类型的事件。
   * 这个示例展示如何只读取UPDATE类型的事件。
   * </p>
   */
  public void filteringEventsExample() {
    System.out.println("\n-- 事件过滤示例 --");

    if (eventJournal.isEmpty()) {
      System.out.println("事件日志为空，请先运行setupEventJournalMap()");
      return;
    }

    long oldestSequence = eventJournal.get(0).getSequence();

    // 创建一个过滤器，只接受UPDATED类型的事件
    Predicate<MapEventItem<String, String>> updateOnlyFilter = event -> event.getType() == EventType.UPDATED;

    // 从journal中读取事件，应用过滤器
    ReadResultSet<MapEventItem<String, String>> events = readFromJournal(oldestSequence, 100, updateOnlyFilter, null);

    // 显示过滤后的事件
    System.out.println("\n仅UPDATE类型的事件 (总计 " + events.size() + "):");
    for (MapEventItem<String, String> event : events) {
      System.out.println("事件类型: " + event.getType() +
          ", 键: " + event.getKey() +
          ", 旧值: " + event.getOldValue() +
          ", 新值: " + event.getNewValue());
    }
  }

  /**
   * 事件投影示例
   * 
   * <p>
   * 演示如何使用投影函数转换Event Journal中的事件。
   * 这个示例将事件转换为简单的字符串描述。
   * </p>
   */
  public void projectionExample() {
    System.out.println("\n-- 事件投影示例 --");

    if (eventJournal.isEmpty()) {
      System.out.println("事件日志为空，请先运行setupEventJournalMap()");
      return;
    }

    long oldestSequence = eventJournal.get(0).getSequence();

    // 创建一个投影，将事件转换为字符串描述
    Function<MapEventItem<String, String>, String> projection = event -> {
      switch (event.getType()) {
        case ADDED:
          return "添加了 " + event.getKey() + " = " + event.getNewValue();
        case UPDATED:
          return "更新了 " + event.getKey() + " 从 " + event.getOldValue() + " 到 " + event.getNewValue();
        case REMOVED:
          return "删除了 " + event.getKey() + " (值为 " + event.getOldValue() + ")";
        case EVICTED:
          return "驱逐了 " + event.getKey() + " (值为 " + event.getOldValue() + ")";
        case EXPIRED:
          return "过期了 " + event.getKey() + " (值为 " + event.getOldValue() + ")";
        default:
          return "未知事件 " + event.getType() + " 对于键 " + event.getKey();
      }
    };

    // 从journal读取事件并应用投影
    ReadResultSet<String> events = readFromJournal(oldestSequence, 100, null, projection);

    // 显示转换后的事件
    System.out.println("\n事件描述 (总计 " + events.size() + "):");
    for (String description : events) {
      System.out.println(description);
    }
  }

  /**
   * 从特定序列号读取示例
   * 
   * <p>
   * 演示如何从特定序列号开始读取Event Journal中的事件。
   * 这对于实现断点续传或在应用程序重启后继续处理很有用。
   * </p>
   */
  public void readingFromSequenceExample() {
    System.out.println("\n-- 从特定序列号读取示例 --");

    if (eventJournal.isEmpty()) {
      System.out.println("事件日志为空，请先运行setupEventJournalMap()");
      return;
    }

    long oldestSequence = eventJournal.get(0).getSequence();
    long newestSequence = eventJournal.get(eventJournal.size() - 1).getSequence();

    // 计算中间点
    long middleSequence = oldestSequence + (newestSequence - oldestSequence) / 2;

    System.out.println("最老序列号: " + oldestSequence);
    System.out.println("最新序列号: " + newestSequence);
    System.out.println("选择的中间序列号: " + middleSequence);

    // 从中间序列号开始读取
    ReadResultSet<MapEventItem<String, String>> events = readFromJournal(middleSequence, 100, null, null);

    // 显示读取的事件
    System.out.println("\n从序列号 " + middleSequence + " 开始读取的事件 (总计 " + events.size() + "):");
    for (MapEventItem<String, String> event : events) {
      System.out.println("序列号: " + event.getSequence() +
          ", 事件类型: " + event.getType() +
          ", 键: " + event.getKey());
    }
  }

  /**
   * 实时事件监控示例
   * 
   * <p>
   * 演示如何实时监控Event Journal中的新事件。
   * 此方法执行一些Map操作，并同时监控和显示生成的事件。
   * </p>
   */
  public void liveEventMonitoringExample() {
    System.out.println("\n-- 实时事件监控示例 --");

    IMap<String, String> map = hazelcastInstance.getMap(MAP_NAME);

    // 清除现有数据和事件
    map.clear();
    eventJournal.clear();
    nextSequence = 0;

    // 重新添加监听器（以防之前的监听器被移除）
    final UUID listenerId1 = map.addEntryListener((EntryAddedListener<String, String>) event -> {
      eventJournal.add(new MapEventItem<>(
          EventType.ADDED,
          event.getKey(),
          null,
          event.getValue(),
          nextSequence++));
    }, true);

    final UUID listenerId2 = map.addEntryListener((EntryUpdatedListener<String, String>) event -> {
      eventJournal.add(new MapEventItem<>(
          EventType.UPDATED,
          event.getKey(),
          event.getOldValue(),
          event.getValue(),
          nextSequence++));
    }, true);

    final UUID listenerId3 = map.addEntryListener((EntryRemovedListener<String, String>) event -> {
      eventJournal.add(new MapEventItem<>(
          EventType.REMOVED,
          event.getKey(),
          event.getOldValue(),
          null,
          nextSequence++));
    }, true);

    try {
      // 获取初始序列号
      final long initialSequence = nextSequence;

      // 创建一个线程来监控事件
      final AtomicInteger eventCount = new AtomicInteger(0);
      Thread monitorThread = new Thread(() -> {
        try {
          long currentSequence = initialSequence;

          System.out.println("开始从序列号 " + currentSequence + " 监控事件");

          // 持续监控10秒
          long endTime = System.currentTimeMillis() + 10000;

          while (System.currentTimeMillis() < endTime) {
            // 从当前序列号读取新事件
            ReadResultSet<MapEventItem<String, String>> events = readFromJournal(currentSequence, 10, null, null);

            // 处理事件
            if (events.size() > 0) {
              for (MapEventItem<String, String> event : events) {
                eventCount.incrementAndGet();
                System.out.println("监测到新事件 -> 序列号: " + event.getSequence() +
                    ", 类型: " + event.getType() +
                    ", 键: " + event.getKey());

                // 更新序列号，为下一次读取做准备
                currentSequence = event.getSequence() + 1;
              }
            }

            // 短暂休眠避免CPU过载
            TimeUnit.MILLISECONDS.sleep(100);
          }

          System.out.println("事件监控完成，共监测到 " + eventCount.get() + " 个事件");

        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          System.err.println("监控事件时出错: " + e.getMessage());
        }
      });

      // 启动监控线程
      monitorThread.start();

      // 在监控的同时执行一些Map操作
      System.out.println("执行Map操作生成事件...");

      // 休眠一下，确保监控线程已经准备好
      TimeUnit.MILLISECONDS.sleep(500);

      // 添加一些条目
      for (int i = 1; i <= 5; i++) {
        String key = "liveKey" + i;
        map.put(key, "值" + i);
        TimeUnit.MILLISECONDS.sleep(300);
      }

      // 更新一些条目
      for (int i = 1; i <= 3; i++) {
        String key = "liveKey" + i;
        map.put(key, "新值" + i);
        TimeUnit.MILLISECONDS.sleep(300);
      }

      // 删除一些条目
      for (int i = 4; i <= 5; i++) {
        String key = "liveKey" + i;
        map.remove(key);
        TimeUnit.MILLISECONDS.sleep(300);
      }

      // 等待监控线程完成
      monitorThread.join();

    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      System.err.println("实时监控示例执行出错: " + e.getMessage());
    } finally {
      // 移除监听器
      map.removeEntryListener(listenerId1);
      map.removeEntryListener(listenerId2);
      map.removeEntryListener(listenerId3);
    }
  }

  /**
   * 从模拟Event Journal读取事件
   * 
   * @param startSequence 开始读取的序列号
   * @param maxItems      最大读取项数
   * @param predicate     过滤器（可以为null）
   * @param projection    投影函数（可以为null）
   * @return 结果集
   */
  private <R> ReadResultSet<R> readFromJournal(
      long startSequence,
      int maxItems,
      Predicate<MapEventItem<String, String>> predicate,
      Function<MapEventItem<String, String>, R> projection) {

    // 找到起始位置
    int startIndex = -1;
    for (int i = 0; i < eventJournal.size(); i++) {
      if (eventJournal.get(i).getSequence() >= startSequence) {
        startIndex = i;
        break;
      }
    }

    if (startIndex == -1) {
      // 没有找到匹配的序列号，返回空结果
      return new ReadResultSetImpl<>(new ArrayList<>(), 0);
    }

    // 提取符合条件的事件
    List<MapEventItem<String, String>> filteredEvents = new ArrayList<>();
    int count = 0;

    for (int i = startIndex; i < eventJournal.size() && count < maxItems; i++) {
      MapEventItem<String, String> event = eventJournal.get(i);
      if (predicate == null || predicate.test(event)) {
        filteredEvents.add(event);
        count++;
      }
    }

    // 如果有投影，则应用投影
    if (projection != null) {
      List<R> projectedEvents = new ArrayList<>(filteredEvents.size());
      for (MapEventItem<String, String> event : filteredEvents) {
        projectedEvents.add(projection.apply(event));
      }
      return new ReadResultSetImpl<>(projectedEvents, count);
    } else {
      // 不安全的类型转换，但因为我们控制了这个方法的所有调用，所以是安全的
      @SuppressWarnings("unchecked")
      List<R> resultList = (List<R>) filteredEvents;
      return new ReadResultSetImpl<>(resultList, count);
    }
  }

  /**
   * 读取结果集实现
   */
  private class ReadResultSetImpl<T> implements ReadResultSet<T> {
    private final List<T> items;
    private final long readCount;

    public ReadResultSetImpl(List<T> items, long readCount) {
      this.items = items;
      this.readCount = readCount;
    }

    @Override
    public int size() {
      return items.size();
    }

    @Override
    public long readCount() {
      return readCount;
    }

    @Override
    public Iterator<T> iterator() {
      return items.iterator();
    }

    @Override
    public long getSequence(T item) {
      if (item instanceof MapEventItem) {
        return ((MapEventItem<?, ?>) item).getSequence();
      }
      // 如果是投影后的项目，则找到对应的索引并返回原始序列号
      int index = items.indexOf(item);
      if (index >= 0 && index < items.size()) {
        // 这里是一个近似值，不过对于演示够用了
        return index;
      }
      return -1;
    }
  }

  /**
   * 实用工具类：读取结果集
   */
  public interface ReadResultSet<T> extends Iterable<T> {
    int size();

    long readCount();

    long getSequence(T item);
  }
}