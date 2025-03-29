package org.example.hazelcast.demo.ap.queue;

import com.hazelcast.collection.IQueue;
import com.hazelcast.core.HazelcastInstance;
import org.springframework.stereotype.Component;

import java.util.Comparator;
import java.util.concurrent.TimeUnit;

@Component
public class QueuePriorityDemo {

  private final HazelcastInstance hazelcastInstance;

  public QueuePriorityDemo(HazelcastInstance hazelcastInstance) {
    this.hazelcastInstance = hazelcastInstance;
  }

  public void runAllExamples() {
    System.out.println("\n开始优先级队列示例...");
    priorityQueueExample();
    System.out.println("\n优先级队列示例完成！");
  }

  /**
   * 使用自定义优先级的队列示例
   */
  public void priorityQueueExample() {
    // 在Hazelcast中创建一个优先级队列
    // 注意：在实际应用中，优先级需要在配置文件中设置，这里仅作为示例展示
    System.out.println("\n=== 优先级队列示例 ===");

    IQueue<PriorityItem> priorityQueue = hazelcastInstance.getQueue("priority-demo-queue");

    try {
      // 清空队列，确保演示从空队列开始
      priorityQueue.clear();

      System.out.println("添加不同优先级的项目到队列");

      // 添加不同优先级的项目
      priorityQueue.add(new PriorityItem("任务 D", 4)); // 最低优先级
      priorityQueue.add(new PriorityItem("任务 B", 8));
      priorityQueue.add(new PriorityItem("任务 A", 10)); // 最高优先级
      priorityQueue.add(new PriorityItem("任务 C", 6));

      System.out.println("队列大小: " + priorityQueue.size());

      System.out.println("\n开始poll项目 (按优先级顺序):");

      // 在Hazelcast中，优先级队列配置了比较器后，会按照优先级顺序返回
      // 注意：由于我们没有配置优先级比较器，这里实际上还是FIFO顺序
      // 这里仅作为概念演示，在实际应用中需要在XML或YAML中配置priority-comparator-class-name
      PriorityItem item;
      while ((item = priorityQueue.poll(1, TimeUnit.SECONDS)) != null) {
        System.out.println("处理: " + item.getName() + ", 优先级: " + item.getPriority());
      }

      System.out.println("\n解释：由于我们没有在Hazelcast配置中设置优先级比较器，项目是按FIFO顺序处理的");
      System.out.println("在实际应用中，您需要在配置文件中设置priority-comparator-class-name");

      // 解释如何在实际中配置优先级队列
      System.out.println("\n如何配置优先级队列：");
      System.out.println("1. 创建一个实现Comparator接口的比较器类");
      System.out.println("2. 在Hazelcast配置文件中设置priority-comparator-class-name");
      System.out.println("XML 示例:");
      System.out.println("<queue name=\"priority-queue\">");
      System.out.println(
          "    <priority-comparator-class-name>com.example.PriorityComparator</priority-comparator-class-name>");
      System.out.println("</queue>");

    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      System.err.println("Queue操作被中断: " + e.getMessage());
    } finally {
      // 清理资源
      priorityQueue.destroy();
    }
  }

  /**
   * 演示用的优先级项目类
   */
  public static class PriorityItem implements Comparable<PriorityItem> {
    private final String name;
    private final int priority;

    public PriorityItem(String name, int priority) {
      this.name = name;
      this.priority = priority;
    }

    public String getName() {
      return name;
    }

    public int getPriority() {
      return priority;
    }

    @Override
    public int compareTo(PriorityItem other) {
      // 高优先级的项应该排在前面
      return Integer.compare(other.priority, this.priority);
    }

    @Override
    public String toString() {
      return "PriorityItem{name='" + name + "', priority=" + priority + '}';
    }
  }

  /**
   * 优先级比较器示例类，可以在Hazelcast配置中使用
   */
  public static class PriorityComparator implements Comparator<Object> {
    @Override
    public int compare(Object o1, Object o2) {
      if (o1 instanceof PriorityItem && o2 instanceof PriorityItem) {
        PriorityItem item1 = (PriorityItem) o1;
        PriorityItem item2 = (PriorityItem) o2;
        // 高优先级的项应该排在前面
        return Integer.compare(item2.getPriority(), item1.getPriority());
      }
      return 0;
    }
  }
}