package org.example.hazelcast.demo.controller;

import com.hazelcast.core.HazelcastInstance;
import org.example.hazelcast.demo.model.User;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

/**
 * 用户控制器 - 基本版本
 */
@RestController
@RequestMapping("/user")
public class UserController {

  @Autowired
  private HazelcastInstance hazelcastInstance;

  @PostMapping
  public String save(@RequestBody User user) {
    hazelcastInstance.getMap("user-map").put(user.id(), user);
    return "Saved to Hazelcast (and DB via MapStore)";
  }

  @GetMapping("/{id}")
  public User get(@PathVariable String id) {
    return (User) hazelcastInstance.getMap("user-map").get(id);
  }
}