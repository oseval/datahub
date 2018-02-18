package ru.oseval.datahub;

import ru.oseval.datahub.data.j.DataOps;

import java.util.HashSet;
import java.util.Set;

public abstract class AbstractEntity {
    private String id;
    private DataOps ops;
    /**
     * Kinds of subscribers that can't be subscribed without explicitly checking by producer
     */
    private Set<String> untrustedKinds;

    AbstractEntity(String id, DataOps ops, Set<String> untrustedKinds) {
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