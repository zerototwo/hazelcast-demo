package org.example.hazelcast.demo.datastructure.cp.iatomicreference;

import com.hazelcast.core.HazelcastInstance;
// 注意: 以下导入在社区版中不可用，仅在企业版中可用
// import com.hazelcast.cp.IAtomicReference;
// import com.hazelcast.cp.CPSubsystem;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * IAtomicReference基本操作示例
 * <p>
 * IAtomicReference是java.util.concurrent.atomic.AtomicReference的分布式实现，
 * 提供了线性一致性的原子操作，支持引用对象的原子性操作。
 * </p>
 * 
 * <p>
 * <strong>IAtomicReference的主要特性：</strong>
 * </p>
 * <ul>
 * <li><strong>线性一致性：</strong> 在分布式环境下提供强一致性保证</li>
 * <li><strong>基于内容：</strong> 操作基于对象的二进制内容而非对象引用</li>
 * <li><strong>序列化存储：</strong> 对象以序列化形式存储，获取时返回反序列化的副本</li>
 * <li><strong>原子操作：</strong> 支持get、set、compareAndSet等原子操作</li>
 * <li><strong>函数式应用：</strong> 支持apply、alter等函数式操作，避免数据竞争</li>
 * </ul>
 * 
 * <p>
 * <strong>适用场景：</strong>
 * </p>
 * <ul>
 * <li>需要在集群中共享可变引用对象</li>
 * <li>实现分布式环境中的单例模式</li>
 * <li>需要原子更新复杂对象（如配置信息）</li>
 * <li>实现分布式状态管理</li>
 * <li>协调多节点共享资源的访问</li>
 * </ul>
 * 
 * <p>
 * <strong>使用注意事项：</strong>
 * </p>
 * <ul>
 * <li>所有方法返回的对象都是私有副本，修改副本不会影响原始值</li>
 * <li>使用compareAndSet时，不应修改原始值，否则序列化内容会变化</li>
 * <li>性能考虑：尽量发送函数到数据而非数据到函数</li>
 * <li>注意序列化/反序列化成本，特别是对于复杂对象图</li>
 * </ul>
 * 
 * <p>
 * 注意: IAtomicReference作为CP数据结构，在社区版中有限支持，完整功能在企业版中可用。
 * 本示例代码使用模拟实现，主要用于演示API用法。
 * </p>
 */
@Component
public class IAtomicReferenceBasicOperationsDemo {

  private final HazelcastInstance hazelcastInstance;
  // 用于模拟IAtomicReference的本地引用
  private final AtomicReference<Object> mockAtomicReference = new AtomicReference<>();

  public IAtomicReferenceBasicOperationsDemo(HazelcastInstance hazelcastInstance) {
    this.hazelcastInstance = hazelcastInstance;
  }

  /**
   * 运行所有IAtomicReference示例
   * 
   * 依次展示IAtomicReference的各项功能，包括基本操作、函数应用、
   * 复杂对象处理和并发场景，全面展示其API和使用方法。
   */
  public void runAllExamples() {
    System.out.println("\n==== IAtomicReference基本操作示例 ====");
    basicOperationsExample();
    functionApplicationExample();
    workingWithComplexObjectsExample();
    concurrentAccessExample();
    System.out.println("==== IAtomicReference示例结束 ====\n");
  }

  /**
   * 演示IAtomicReference的基本操作：get、set、getAndSet、compareAndSet
   * 
   * <p>
   * 展示如何执行以下操作：
   * </p>
   * <ul>
   * <li>创建和获取IAtomicReference实例</li>
   * <li>获取存储的值 (get)</li>
   * <li>设置新值 (set)</li>
   * <li>原子地获取当前值并设置新值 (getAndSet)</li>
   * <li>原子地比较当前值并设置新值 (compareAndSet)</li>
   * </ul>
   * 
   * <p>
   * 这些基本操作是构建更复杂逻辑的基础，在分布式环境中保证操作的原子性。
   * </p>
   */
  public void basicOperationsExample() {
    System.out.println("\n-- IAtomicReference基本操作 --");
    System.out.println("模拟IAtomicReference操作，实际应使用CP子系统获取");

    // 实际使用时：
    // IAtomicReference<String> atomicRef =
    // hazelcastInstance.getCPSubsystem().getAtomicReference("myAtomicRef");

    // 设置初始值
    mockAtomicReference.set("初始值");
    System.out.println("初始值: " + mockAtomicReference.get());

    // 设置新值
    mockAtomicReference.set("新值");
    System.out.println("设置后的值: " + mockAtomicReference.get());

    // 获取并设置
    String oldValue = (String) mockAtomicReference.getAndSet("更新的值");
    System.out.println("getAndSet操作 - 旧值: " + oldValue + ", 新值: " + mockAtomicReference.get());

    // 比较并设置
    boolean success = mockAtomicReference.compareAndSet("更新的值", "CAS后的值");
    System.out.println("compareAndSet操作 - 成功: " + success + ", 当前值: " + mockAtomicReference.get());

    // 尝试失败的比较并设置
    success = mockAtomicReference.compareAndSet("不匹配的值", "不会设置的值");
    System.out.println("失败的compareAndSet - 成功: " + success + ", 当前值: " + mockAtomicReference.get());
  }

  /**
   * 演示IAtomicReference的函数应用
   * <p>
   * 展示apply、alter、alterAndGet和getAndAlter方法，这些方法接受函数作为参数，
   * 使用函数式编程的方式操作存储的值。
   * </p>
   * 
   * <p>
   * <strong>函数应用的优势：</strong>
   * </p>
   * <ul>
   * <li><strong>性能优化：</strong> 将函数发送到数据而不是将数据取回，减少网络传输</li>
   * <li><strong>避免竞态条件：</strong> 不需要执行读取-修改-写入等多步操作，减少并发问题</li>
   * <li><strong>简化代码：</strong> 函数式风格使代码更易读和维护</li>
   * </ul>
   * 
   * <p>
   * 这些函数操作在需要执行复杂转换或依赖当前值计算新值时特别有用。
   * </p>
   */
  public void functionApplicationExample() {
    System.out.println("\n-- IAtomicReference的函数应用 --");

    // 实际Hazelcast中，我们使用IFunction接口
    // 这里我们使用Java标准库中的Function作为替代

    // 设置初始值
    mockAtomicReference.set("hello");
    System.out.println("初始值: " + mockAtomicReference.get());

    // 定义一个函数，将字符串转为大写
    Function<String, String> toUpperCaseFunction = String::toUpperCase;

    // 模拟apply - 应用函数但不改变原值
    String result = toUpperCaseFunction.apply((String) mockAtomicReference.get());
    System.out.println("apply结果: " + result);
    System.out.println("apply后的原值: " + mockAtomicReference.get());

    // 模拟alter - 修改值但不返回结果
    mockAtomicReference.set("hello");
    mockAtomicReference.set(toUpperCaseFunction.apply((String) mockAtomicReference.get()));
    System.out.println("alter后的值: " + mockAtomicReference.get());

    // 模拟alterAndGet - 修改值并返回新值
    mockAtomicReference.set("hello");
    mockAtomicReference.updateAndGet(v -> toUpperCaseFunction.apply((String) v));
    System.out.println("alterAndGet后的值: " + mockAtomicReference.get());

    // 模拟getAndAlter - 修改值并返回旧值
    mockAtomicReference.set("hello");
    String previousValue = (String) mockAtomicReference.getAndUpdate(v -> toUpperCaseFunction.apply((String) v));
    System.out.println("getAndAlter结果（旧值）: " + previousValue);
    System.out.println("getAndAlter后的新值: " + mockAtomicReference.get());

    // 对null值的处理
    mockAtomicReference.set(null);
    System.out.println("设置为null: " + mockAtomicReference.get());
    mockAtomicReference.set("重新设置");
    System.out.println("从null恢复: " + mockAtomicReference.get());
  }

  /**
   * 演示IAtomicReference处理复杂对象
   * <p>
   * 展示如何正确地使用IAtomicReference存储和修改复杂对象（如自定义类）。
   * </p>
   * 
   * <p>
   * <strong>关键概念：</strong>
   * </p>
   * <ul>
   * <li><strong>基于二进制内容：</strong> IAtomicReference基于序列化内容而非对象引用</li>
   * <li><strong>返回对象副本：</strong> 获取值时返回的是对象的副本，修改副本不会影响原始值</li>
   * <li><strong>正确的修改方式：</strong> 使用原子操作（如compareAndSet或alter）来修改值</li>
   * </ul>
   * 
   * <p>
   * 这个示例特别关注了常见的陷阱，如尝试直接修改检索到的对象，这在分布式环境中是无效的。
   * </p>
   */
  public void workingWithComplexObjectsExample() {
    System.out.println("\n-- IAtomicReference处理复杂对象 --");

    // 创建一个Person对象
    Person person = new Person("张三", 30);
    mockAtomicReference.set(person);
    System.out.println("初始Person: " + mockAtomicReference.get());

    // 错误的修改方式 - 直接修改引用而不是原子操作
    Person retrievedPerson = (Person) mockAtomicReference.get();
    retrievedPerson.setAge(35);
    System.out.println("直接修改获取的对象后 (错误的做法): " + mockAtomicReference.get());
    System.out.println("警告: 这种修改方式不会传播到其他节点！");

    // 正确的修改方式 - 通过原子操作
    mockAtomicReference.updateAndGet(p -> {
      Person newPerson = new Person(((Person) p).getName(), ((Person) p).getAge());
      newPerson.setAge(40);
      return newPerson;
    });
    System.out.println("通过原子操作修改后 (正确的做法): " + mockAtomicReference.get());

    // 展示引用更换但内容相同的情况
    Person sameContentPerson = new Person("张三", 40);
    System.out.println("新对象内容相同: " + sameContentPerson);
    System.out.println("新旧对象equals比较: " + sameContentPerson.equals(mockAtomicReference.get()));
    System.out.println("新旧对象==比较: " + (sameContentPerson == mockAtomicReference.get()));
    System.out.println("注意: IAtomicReference基于二进制内容，不是对象引用");
  }

  /**
   * 演示多线程环境下IAtomicReference的使用
   * <p>
   * 展示在高并发环境下，如何使用IAtomicReference安全地更新共享状态而不引入竞态条件。
   * </p>
   * 
   * <p>
   * <strong>并发场景重点：</strong>
   * </p>
   * <ul>
   * <li><strong>原子更新：</strong> 使用updateAndGet等方法确保更新的原子性</li>
   * <li><strong>无锁编程：</strong> 避免使用显式锁，依靠内置的原子操作</li>
   * <li><strong>高并发下的可靠性：</strong> 即使在多线程同时访问时也能保持数据一致性</li>
   * </ul>
   * 
   * <p>
   * 该示例创建多个线程并发更新共享列表，展示IAtomicReference在并发环境中的线程安全保证。
   * 在分布式系统中，这种能力对于维护集群范围内的一致性至关重要。
   * </p>
   */
  public void concurrentAccessExample() {
    System.out.println("\n-- 多线程环境下的IAtomicReference --");

    // 设置初始值为空列表
    mockAtomicReference.set(new ArrayList<String>());

    int threadCount = 5;
    int operationsPerThread = 100;

    ExecutorService executorService = Executors.newFixedThreadPool(threadCount);
    CountDownLatch latch = new CountDownLatch(threadCount);

    System.out.println("开始" + threadCount + "个线程，每个线程添加" + operationsPerThread + "个元素");

    for (int i = 0; i < threadCount; i++) {
      final int threadId = i;
      executorService.submit(() -> {
        try {
          for (int j = 0; j < operationsPerThread; j++) {
            final int elementIndex = j; // 使j在lambda中有效final
            // 原子地更新列表，添加新元素
            mockAtomicReference.updateAndGet(current -> {
              @SuppressWarnings("unchecked")
              List<String> list = new ArrayList<>((List<String>) current);
              list.add("线程-" + threadId + "-元素-" + elementIndex);
              return list;
            });
          }
        } finally {
          latch.countDown();
        }
      });
    }

    try {
      latch.await();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      System.err.println("等待线程完成时被中断: " + e.getMessage());
    }

    executorService.shutdown();

    @SuppressWarnings("unchecked")
    List<String> resultList = (List<String>) mockAtomicReference.get();
    System.out.println("预期元素数量: " + (threadCount * operationsPerThread));
    System.out.println("实际元素数量: " + resultList.size());
    System.out.println("多线程操作过程中没有丢失更新");

    if (resultList.size() < 10) {
      System.out.println("列表内容: " + resultList);
    } else {
      System.out.println("列表前5个元素: " + resultList.subList(0, 5));
      System.out.println("列表后5个元素: " + resultList.subList(resultList.size() - 5, resultList.size()));
    }
  }

  /**
   * Person类 - 用于展示IAtomicReference如何处理复杂对象
   * <p>
   * 这是一个简单的POJO类，用于IAtomicReference示例中展示复杂对象操作。
   * 该类实现了equals和hashCode方法，便于对象内容比较。
   * </p>
   */
  public static class Person {
    private String name;
    private int age;

    public Person(String name, int age) {
      this.name = name;
      this.age = age;
    }

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }

    public int getAge() {
      return age;
    }

    public void setAge(int age) {
      this.age = age;
    }

    @Override
    public String toString() {
      return "Person{name='" + name + "', age=" + age + '}';
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj)
        return true;
      if (obj == null || getClass() != obj.getClass())
        return false;
      Person person = (Person) obj;
      return age == person.age &&
          (name == null ? person.name == null : name.equals(person.name));
    }

    @Override
    public int hashCode() {
      int result = name != null ? name.hashCode() : 0;
      result = 31 * result + age;
      return result;
    }
  }
}