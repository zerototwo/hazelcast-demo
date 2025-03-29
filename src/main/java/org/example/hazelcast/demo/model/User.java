package org.example.hazelcast.demo.model;

import java.io.Serializable;

public record User(String id, String name, String email) {}