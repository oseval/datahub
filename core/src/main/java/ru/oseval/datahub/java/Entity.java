package ru.oseval.datahub.java;

import ru.oseval.datahub.data.java.DataOps;

import java.util.HashSet;
import java.util.Set;

abstract class Entity {
    private String id;
    private DataOps ops;
    /**
     * Kinds of subscribers that can't be subscribed without explicitly checking by producer
     */
    private Set<String> untrustedKinds;

    Entity(String id, DataOps ops, Set<String> untrustedKinds) {
      this.id = id;
      this.ops = ops;
      this.untrustedKinds = untrustedKinds == null ? new HashSet<>() : untrustedKinds;
    }

    public String getId() {
        return id;
    }

    public DataOps getOps() {
        return ops;
    }

    public Set<String> getUntrustedKinds() {
        return untrustedKinds;
    }
}