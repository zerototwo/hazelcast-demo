package org.example.hazelcast.demo.store;

import com.hazelcast.map.MapStore;
import org.example.hazelcast.demo.model.User;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;

@Repository
public class UserMapStore implements MapStore<String, User> {

    private static final Logger logger = LoggerFactory.getLogger(UserMapStore.class);
    private static final int BATCH_SIZE = 50;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Override
    public void store(String key, User user) {
        logger.debug("Storing user with ID: {}", key);
        jdbcTemplate.update("REPLACE INTO users (id, name, email) VALUES (?, ?, ?)",
                user.id(), user.name(), user.email());
    }

    @Override
    public void storeAll(Map<String, User> map) {
        if (map.isEmpty())
            return;

        logger.debug("Batch storing {} users", map.size());
        List<User> users = new ArrayList<>(map.values());
        jdbcTemplate.batchUpdate("REPLACE INTO users (id, name, email) VALUES (?, ?, ?)",
                new BatchPreparedStatementSetter() {
                    public void setValues(PreparedStatement ps, int i) throws SQLException {
                        User u = users.get(i);
                        ps.setString(1, u.id());
                        ps.setString(2, u.name());
                        ps.setString(3, u.email());
                    }

                    public int getBatchSize() {
                        return users.size();
                    }
                });
    }

    @Override
    public User load(String key) {
        logger.debug("Loading user with ID: {}", key);
        try {
            return jdbcTemplate.queryForObject(
                    "SELECT * FROM users WHERE id = ?",
                    new Object[] { key },
                    (rs, rowNum) -> new User(
                            rs.getString("id"),
                            rs.getString("name"),
                            rs.getString("email")));
        } catch (EmptyResultDataAccessException e) {
            logger.debug("User not found with ID: {}", key);
            return null;
        }
    }

    @Override
    public Map<String, User> loadAll(Collection<String> keys) {
        if (keys.isEmpty())
            return Collections.emptyMap();

        logger.debug("Loading {} users", keys.size());
        Map<String, User> result = new HashMap<>();

        // 转换成List以支持分批操作
        List<String> keysList = new ArrayList<>(keys);

        // 分批加载项
        int totalKeys = keysList.size();
        for (int i = 0; i < totalKeys; i += BATCH_SIZE) {
            int endIndex = Math.min(i + BATCH_SIZE, totalKeys);
            List<String> batchKeys = keysList.subList(i, endIndex);
            loadBatch(batchKeys, result);
        }

        return result;
    }

    private void loadBatch(List<String> batchKeys, Map<String, User> resultMap) {
        if (batchKeys.isEmpty())
            return;

        // 构建IN子句
        String placeholders = String.join(",", Collections.nCopies(batchKeys.size(), "?"));
        String sql = "SELECT * FROM users WHERE id IN (" + placeholders + ")";

        List<User> users = jdbcTemplate.query(sql, batchKeys.toArray(),
                (rs, rowNum) -> new User(rs.getString("id"), rs.getString("name"), rs.getString("email")));

        // 保存到结果集
        for (User user : users) {
            resultMap.put(user.id(), user);
        }
    }

    @Override
    public Iterable<String> loadAllKeys() {
        logger.info("Loading all user keys");
        return jdbcTemplate.query("SELECT id FROM users", (rs, rowNum) -> rs.getString("id"));
    }

    @Override
    public void delete(String key) {
        logger.debug("Deleting user with ID: {}", key);
        jdbcTemplate.update("DELETE FROM users WHERE id = ?", key);
    }

    @Override
    public void deleteAll(Collection<String> keys) {
        if (keys.isEmpty())
            return;

        logger.debug("Batch deleting {} users", keys.size());

        // 用于IN子句的批量删除
        String placeholders = String.join(",", Collections.nCopies(keys.size(), "?"));
        String sql = "DELETE FROM users WHERE id IN (" + placeholders + ")";
        jdbcTemplate.update(sql, keys.toArray());
    }

    /**
     * 根据邮箱域名获取用户ID
     */
    public List<String> getUserIdsByEmailDomain(String domain) {
        logger.debug("Loading user IDs by email domain: {}", domain);
        return jdbcTemplate.query(
                "SELECT id FROM users WHERE email LIKE ?",
                new Object[] { "%" + domain },
                (rs, rowNum) -> rs.getString("id"));
    }

    /**
     * 确保users表存在
     */
    public void ensureTableExists() {
        try {
            // 检查表是否存在
            jdbcTemplate.queryForObject("SELECT COUNT(*) FROM users LIMIT 1", Integer.class);
            logger.info("Users表已存在");
        } catch (Exception e) {
            logger.info("创建users表");
            // 创建表
            jdbcTemplate.execute("CREATE TABLE IF NOT EXISTS users (" +
                    "id VARCHAR(100) PRIMARY KEY, " +
                    "name VARCHAR(255), " +
                    "email VARCHAR(255))");
        }
    }
}