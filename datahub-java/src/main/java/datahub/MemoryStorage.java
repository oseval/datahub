package ru.oseval.datahub.j;

import java.util.Comparator;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;

public class MemoryStorage implements Datahub.Storage {
    private ConcurrentMap<String, Object> ids = new ConcurrentHashMap<String, Object>();

    @Override
    public <C> void increase(String entityId, C dataClock, Comparator<C> cmp, Function<Void, Void> callback) {
        Object res = ids.computeIfPresent(entityId, (k, _clk) -> {
            C clk = (C) _clk;
            return cmp.compare(dataClock, clk) > 0 ? dataClock : clk;
        });

        if (res == null) {
            ids.putIfAbsent(entityId, dataClock);
        }
    }

    @Override
    public <C> void getLastClock(String entityId, Function<Optional<C>, Void> callback) {
        callback.apply(Optional.ofNullable((C) ids.get(entityId)));
    }
}
