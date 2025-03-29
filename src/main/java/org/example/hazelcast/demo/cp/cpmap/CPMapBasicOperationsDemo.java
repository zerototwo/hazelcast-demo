package org.example.hazelcast.demo.cp.cpmap;

import com.hazelcast.core.HazelcastInstance;
// 注意: 以下导入在社区版中不可用，仅在企业版中可用
// import com.hazelcast.cp.CPMap;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

/**
 * CPMap基本操作示例
 * CPMap是一个分布式的键值存储接口实现，作为CP数据结构，它保证了数据的一致性，
 * 即使在网络分区期间也能确保每个集群成员始终看到相同的数据。
 * 
 * 注意: CPMap功能仅在Hazelcast企业版中可用。
 * 本示例代码使用模拟实现，仅用于演示API用法，无法在社区版中实际运行。
 */
@Component
public class CPMapBasicOperationsDemo {

  private final HazelcastInstance hazelcastInstance;

  public CPMapBasicOperationsDemo(HazelcastInstance hazelcastInstance) {
    this.hazelcastInstance = hazelcastInstance;
  }

  /**
   * 运行所有CPMap示例
   */
  public void runAllExamples() {
    System.out.println("\n==== CPMap基本操作示例 (仅企业版支持) ====");
    System.out.println("注意: 以下示例仅在Hazelcast企业版中可用，当前为模拟演示");
    basicOperationsExample();
    atomicOperationsExample();
    System.out.println("==== CPMap示例结束 ====\n");
  }

  /**
   * 演示CPMap的基本操作
   */
  public void basicOperationsExample() {
    System.out.println("\n-- CPMap基本操作 --");
    System.out.println("模拟CPMap操作，实际需要企业版支持");

    // 模拟操作，实际在企业版中会使用:
    // CPMap<String, String> capitalCities =
    // hazelcastInstance.getCPSubsystem().getMap("capitalCities");

    // 使用HashMap模拟操作
    Map<String, String> capitalCities = new HashMap<>();

    try {
      // 当不需要获取与键关联的先前值时，优先使用'set'而不是'put'
      // capitalCities.set("England", "London");
      capitalCities.put("England", "London"); // 模拟set操作
      System.out.println("设置England的首都为: London");

      String englandCapital = capitalCities.get("England");
      System.out.println("获取England的首都: " + englandCapital);

      // put操作会返回之前的值
      String previousValue = capitalCities.put("France", "Paris");
      System.out.println("设置France的首都为Paris, 之前的值: " + previousValue);

      // remove操作会返回被删除的值
      String removedValue = capitalCities.remove("England");
      System.out.println("移除England的首都: " + removedValue);

      // 当不需要获取被删除键的值时，优先使用'delete'而不是'remove'
      // capitalCities.delete("France");
      capitalCities.remove("France"); // 模拟delete操作
      System.out.println("删除France的首都");

      // 验证数据已被删除
      System.out.println("验证England是否存在: " + (capitalCities.get("England") != null));
      System.out.println("验证France是否存在: " + (capitalCities.get("France") != null));
    } finally {
      // 不再使用的CPMap实例应该被销毁，以避免内存泄漏
      // capitalCities.destroy();
      capitalCities.clear(); // 模拟destroy操作
      System.out.println("CPMap实例已销毁 (模拟)");
    }
  }

  /**
   * 演示CPMap的原子操作
   */
  public void atomicOperationsExample() {
    System.out.println("\n-- CPMap原子操作 --");
    System.out.println("模拟CPMap原子操作，实际需要企业版支持");

    // 模拟操作，实际在企业版中会使用:
    // CPMap<String, String> countryCodes =
    // hazelcastInstance.getCPSubsystem().getMap("countryCodes");

    // 使用HashMap模拟操作
    Map<String, String> countryCodes = new HashMap<>();

    try {
      // 设置初始值
      // countryCodes.set("Germany", "Munich");
      countryCodes.put("Germany", "Munich"); // 模拟set操作
      System.out.println("设置Germany的值为: Munich");

      // 使用compareAndSet进行原子更新
      // boolean updated = countryCodes.compareAndSet("Germany", "Munich", "Berlin");
      boolean updated = false;
      // 模拟compareAndSet
      if (countryCodes.get("Germany") != null && countryCodes.get("Germany").equals("Munich")) {
        countryCodes.put("Germany", "Berlin");
        updated = true;
      }
      System.out.println("使用compareAndSet更新Germany的值 (Munich -> Berlin): " + updated);
      System.out.println("现在Germany的值为: " + countryCodes.get("Germany"));

      // 尝试使用错误的期望值进行原子更新
      // updated = countryCodes.compareAndSet("Germany", "Munich", "Hamburg");
      updated = false;
      // 模拟错误的compareAndSet
      if (countryCodes.get("Germany") != null && countryCodes.get("Germany").equals("Munich")) {
        countryCodes.put("Germany", "Hamburg");
        updated = true;
      }
      System.out.println("尝试使用错误的期望值更新Germany (Munich -> Hamburg): " + updated);
      System.out.println("现在Germany的值仍为: " + countryCodes.get("Germany"));

      // 使用putIfAbsent，只有在键不存在时才设置值
      // String previousValue = countryCodes.putIfAbsent("Ireland", "Dublin");
      String previousValue = countryCodes.putIfAbsent("Ireland", "Dublin");
      System.out.println("使用putIfAbsent设置Ireland为Dublin, 之前的值: " + previousValue);

      // 尝试对已存在的键使用putIfAbsent
      // previousValue = countryCodes.putIfAbsent("Ireland", "Cork");
      previousValue = countryCodes.putIfAbsent("Ireland", "Cork");
      System.out.println("尝试使用putIfAbsent将Ireland设为Cork, 之前的值: " + previousValue);
      System.out.println("Ireland的值仍为: " + countryCodes.get("Ireland"));
    } finally {
      // 不再使用的CPMap实例应该被销毁，以避免内存泄漏
      // countryCodes.destroy();
      countryCodes.clear(); // 模拟destroy操作
      System.out.println("CPMap实例已销毁 (模拟)");
    }
  }
}