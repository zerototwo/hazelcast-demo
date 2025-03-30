package org.example.hazelcast.demo.config;

import com.hazelcast.config.*;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import org.example.hazelcast.demo.store.ProductMapStore;
import org.example.hazelcast.demo.store.UserMapStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class HazelcastConfiguration {

    @Bean
    public Config hazelcastConfig(UserMapStore userMapStore, ProductMapStore productMapStore) {
        // 产品Map配置
        MapConfig productMapConfig = new MapConfig("products")
                .setBackupCount(1)
                .setReadBackupData(true) // 允许从备份读取数据，提高性能
                .setMapStoreConfig(new MapStoreConfig()
                        .setImplementation(productMapStore)
                        .setWriteDelaySeconds(2) // 添加小的写入延迟，允许批量操作
                        .setWriteBatchSize(50) // 批量写入大小
                    .setEnabled(true)
                        .setInitialLoadMode(MapStoreConfig.InitialLoadMode.EAGER)); // 启动时加载

        // 添加产品Map的索引
        productMapConfig.addIndexConfig(new IndexConfig(IndexType.SORTED, "category"));
        productMapConfig.addIndexConfig(new IndexConfig(IndexType.HASH, "price"));

        // 用户Map配置
        MapConfig userMapConfig = new MapConfig("user-map")
                .setBackupCount(1)
                .setMapStoreConfig(new MapStoreConfig()
                        .setImplementation(userMapStore)
                        .setWriteDelaySeconds(0) // 即时写入
                        .setInitialLoadMode(MapStoreConfig.InitialLoadMode.EAGER)); // 启动时加载
//企业版Hazelcast才支持持久化配置
//        PersistenceConfig PersistenceConfig = new PersistenceConfig()
//            .setEnabled(true);

        return new Config()
                .setInstanceName("hazelcast-instance")
                .addMapConfig(userMapConfig)
                .addMapConfig(productMapConfig)
                ;
    }

    @Bean
    public HazelcastInstance hazelcastInstance(Config config) {
        return Hazelcast.newHazelcastInstance(config);
    }
}