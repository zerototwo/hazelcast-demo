package org.example.hazelcast.demo;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Hazelcast;

/**
 * Hazelcast演示应用
 */
@SpringBootApplication
public class HazelcastDemoApplication {

    public static void main(String[] args) {
        SpringApplication.run(HazelcastDemoApplication.class, args);
    }

    @Bean
    public HazelcastInstance hazelcastInstance() {
        return Hazelcast.newHazelcastInstance();
    }

}
