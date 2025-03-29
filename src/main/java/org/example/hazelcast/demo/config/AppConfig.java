package org.example.hazelcast.demo.config;

import org.springframework.boot.CommandLineRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.core.annotation.Order;

/**
 * 应用程序配置
 */
@Configuration
public class AppConfig {

  /**
   * 打印启动信息的CommandLineRunner
   */
  @Bean
  @Order(Integer.MIN_VALUE) // 确保最先执行
  @Primary
  public CommandLineRunner startupInfoRunner() {
    return args -> {
      System.out.println("\n=================================================================");
      System.out.println("                Hazelcast Demo 应用程序启动");
      System.out.println("=================================================================");
      System.out.println("应用程序已启动完成，下面将显示主菜单...");
      System.out.println("=================================================================\n");

      // 这里不做任何操作，让后续的CommandLineRunner展示主菜单
    };
  }
}