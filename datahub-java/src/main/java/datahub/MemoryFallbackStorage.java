package ru.oseval.datahub.j;

import java.util.Comparator;
import java.util.Optional;
import java.util.function.Function;

public class MemoryFallbackStorage extends MemoryStorage {
    private Datahub.Storage storage;

    public MemoryFallbackStorage(Datahub.Storage storage) {
        this.storage = storage;
    }

    @Override
    public <C> void increase(String entityId, C dataClock, Comparator<C> cmp, Function<Void, Void> callback) {
        super.increase(entityId, dataClock, cmp, v -> {
            storage.increase(entityId, dataClock, cmp, callback);
            return null;
        });
    }

    @Override
    public <C> void getLastClock(String entityId, Function<Optional<C>, Void> callback) {
        MemoryStorage self = this;

        storage.getLastClock(entityId, (Function<Optional<C>, Void>) clockOpt -> {
            if (clockOpt.isPresent()) {
                callback.apply(clockOpt);
            } else {
                self.getLastClock(entityId, callback);
            }

            return null;
        });
    }
}
