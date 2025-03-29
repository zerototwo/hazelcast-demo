package org.example.hazelcast.demo.serializing;

import com.hazelcast.config.Config;
import com.hazelcast.config.SerializationConfig;
import com.hazelcast.config.SerializerConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.StreamSerializer;
import com.hazelcast.nio.serialization.compact.CompactReader;
import com.hazelcast.nio.serialization.compact.CompactSerializer;
import com.hazelcast.nio.serialization.compact.CompactWriter;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.Map;

/**
 * 演示Hazelcast中不同序列化方法的示例类
 */
@Component
public class SerializationDemo {
  private final HazelcastInstance hazelcastInstance;

  public SerializationDemo(HazelcastInstance hazelcastInstance) {
    this.hazelcastInstance = hazelcastInstance;
  }

  /**
   * 演示并比较Hazelcast中不同的序列化方法
   */
  public void runDemo() {
    System.out.println("============== Hazelcast序列化演示 ==============");
    System.out.println("本演示展示不同序列化方法的使用方式和特点\n");

    // 演示不同序列化方法
    demoCompactSerialization();
    demoCustomSerialization();
    demoJavaSerialization();

    System.out.println("\n序列化方法比较：");
    System.out.println("1. Compact序列化 - 优化内存使用和效率，支持模式演变");
    System.out.println("2. 自定义序列化 - 灵活的序列化控制，适用于复杂情况");
    System.out.println("3. Java序列化 - 简单但效率较低，仅推荐用于快速原型");
    System.out.println("============== 演示结束 ==============");
  }

  /**
   * 演示Compact序列化
   */
  private void demoCompactSerialization() {
    System.out.println("\n--- Compact序列化演示 ---");

    // 通常在配置中注册序列化器
    // 这里我们仅展示代码结构
    System.out.println("Compact序列化配置示例：");
    System.out.println("SerializationConfig serConfig = new SerializationConfig();");
    System.out.println("serConfig.getCompactSerializationConfig().addSerializer(new EmployeeSerializer());");
    System.out.println("Config config = new Config();");
    System.out.println("config.setSerializationConfig(serConfig);");

    System.out.println("\nCompact序列化优点：");
    System.out.println("- 占用内存少");
    System.out.println("- 序列化/反序列化速度快");
    System.out.println("- 支持版本演变");
    System.out.println("- 支持部分反序列化（用于查询）");
  }

  /**
   * 演示自定义序列化
   */
  private void demoCustomSerialization() {
    System.out.println("\n--- 自定义序列化演示 ---");

    System.out.println("自定义序列化配置示例：");
    System.out.println("SerializationConfig serConfig = new SerializationConfig();");
    System.out.println("serConfig.addSerializerConfig(");
    System.out.println("    new SerializerConfig()");
    System.out.println("        .setImplementation(new EmployeeStreamSerializer())");
    System.out.println("        .setTypeClass(Employee.class));");

    System.out.println("\n自定义序列化优点：");
    System.out.println("- 完全控制序列化过程");
    System.out.println("- 可以处理复杂对象图");
    System.out.println("- 支持所有Hazelcast客户端");
  }

  /**
   * 演示Java原生序列化
   */
  private void demoJavaSerialization() {
    System.out.println("\n--- Java序列化演示 ---");

    System.out.println("Java序列化使用示例：");
    System.out.println("public class Employee implements Serializable {");
    System.out.println("    private static final long serialVersionUID = 1L;");
    System.out.println("    private int id;");
    System.out.println("    private String name;");
    System.out.println("    // 构造函数、getter和setter");
    System.out.println("}");

    System.out.println("\nJava序列化优点：");
    System.out.println("- 无需额外配置");
    System.out.println("- 简单易用");
    System.out.println("- 适用于快速原型设计");

    System.out.println("\nJava序列化缺点：");
    System.out.println("- 序列化性能较差");
    System.out.println("- 序列化结果较大");
    System.out.println("- 仅支持Java客户端");
  }

  /**
   * 实体类示例：员工
   */
  public static class Employee {
    private int id;
    private String name;
    private String department;

    public Employee() {
    }

    public Employee(int id, String name, String department) {
      this.id = id;
      this.name = name;
      this.department = department;
    }

    // Getter和Setter方法
    public int getId() {
      return id;
    }

    public void setId(int id) {
      this.id = id;
    }

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }

    public String getDepartment() {
      return department;
    }

    public void setDepartment(String department) {
      this.department = department;
    }

    @Override
    public String toString() {
      return "Employee{" +
          "id=" + id +
          ", name='" + name + '\'' +
          ", department='" + department + '\'' +
          '}';
    }
  }

  /**
   * Compact序列化器示例
   */
  public static class EmployeeSerializer implements CompactSerializer<Employee> {
    @Override
    public Employee read(CompactReader reader) {
      Employee employee = new Employee();
      employee.setId(reader.readInt32("id"));
      employee.setName(reader.readString("name"));
      employee.setDepartment(reader.readString("department"));
      return employee;
    }

    @Override
    public void write(CompactWriter writer, Employee employee) {
      writer.writeInt32("id", employee.getId());
      writer.writeString("name", employee.getName());
      writer.writeString("department", employee.getDepartment());
    }

    @Override
    public String getTypeName() {
      return "employee";
    }

    @Override
    public Class<Employee> getCompactClass() {
      return Employee.class;
    }
  }

  /**
   * 自定义序列化器示例
   */
  public static class EmployeeStreamSerializer implements StreamSerializer<Employee> {
    @Override
    public void write(ObjectDataOutput out, Employee employee) throws IOException {
      out.writeInt(employee.getId());
      out.writeString(employee.getName());
      out.writeString(employee.getDepartment());
    }

    @Override
    public Employee read(ObjectDataInput in) throws IOException {
      int id = in.readInt();
      String name = in.readString();
      String department = in.readString();
      return new Employee(id, name, department);
    }

    @Override
    public int getTypeId() {
      return 1000; // 自定义类型ID，必须唯一
    }
  }
}