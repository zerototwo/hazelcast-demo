package org.example.hazelcast.demo.config;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import org.example.hazelcast.demo.store.UserMapStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class HazelcastConfiguration {

    @Autowired
    private UserMapStore userMapStore;

    @Bean
    public Config hazelcastConfig() {
        MapStoreConfig mapStoreConfig = new MapStoreConfig();
        mapStoreConfig.setImplementation(userMapStore);
        mapStoreConfig.setWriteDelaySeconds(0); // 0 表示同步写入

        MapConfig userMapConfig = new MapConfig("user-map")
                .setBackupCount(1)
                .setMapStoreConfig(mapStoreConfig);

        return new Config()
                .setInstanceName("hazelcast-instance")
                .addMapConfig(userMapConfig);
    }

    @Bean
    public HazelcastInstance hazelcastInstance() {
        return Hazelcast.newHazelcastInstance(hazelcastConfig());
    }
}