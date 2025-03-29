package org.example.hazelcast.demo.store;

import com.hazelcast.map.MapStore;
import org.example.hazelcast.demo.model.User;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;

@Repository
public class UserMapStore implements MapStore<String, User> {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Override
    public void store(String key, User user) {
        jdbcTemplate.update("REPLACE INTO users (id, name, email) VALUES (?, ?, ?)",
            user.id(), user.name(), user.email());
    }

    @Override
    public void storeAll(Map<String, User> map) {
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
        try {
            return jdbcTemplate.queryForObject(
                "SELECT * FROM users WHERE id = ?",
                new Object[]{key},
                (rs, rowNum) -> new User(
                    rs.getString("id"),
                    rs.getString("name"),
                    rs.getString("email")
                )
            );
        } catch (EmptyResultDataAccessException e) {
            return null;
        }
    }

    @Override
    public Map<String, User> loadAll(Collection<String> keys) {
        if (keys.isEmpty()) return Map.of();
        String placeholders = String.join(",", Collections.nCopies(keys.size(), "?"));
        String sql = "SELECT * FROM users WHERE id IN (" + placeholders + ")";
        List<User> users = jdbcTemplate.query(sql, keys.toArray(), (rs, rowNum) ->
            new User(rs.getString("id"), rs.getString("name"), rs.getString("email"))
        );

        Map<String, User> result = new HashMap<>();
        for (User u : users) {
            result.put(u.id(), u);
        }
        return result;
    }

    @Override
    public Iterable<String> loadAllKeys() {
        return jdbcTemplate.query("SELECT id FROM users", (rs, rowNum) -> rs.getString("id"));
    }

    @Override
    public void delete(String key) {
        jdbcTemplate.update("DELETE FROM users WHERE id = ?", key);
    }

    @Override
    public void deleteAll(Collection<String> keys) {
        if (keys.isEmpty()) return;
        String placeholders = String.join(",", Collections.nCopies(keys.size(), "?"));
        String sql = "DELETE FROM users WHERE id IN (" + placeholders + ")";
        jdbcTemplate.update(sql, keys.toArray());
    }
}