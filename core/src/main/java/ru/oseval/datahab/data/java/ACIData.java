package ru.oseval.datahab.data.java;

import java.util.Comparator;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

abstract class ACIDataOps<A> extends DataOps<Long> {
    private Function<A, Set<String>> relations;
    public ACIDataOps(Function<A, Set<String>> relations) {
        if (relations == null) {
            this.relations = x -> { return new HashSet<>(); };
        } else {
            this.relations = relations;
        }
    }
    Comparator<Long> ordering = Comparator.naturalOrder();
    ACIData<A> zero = new ACIData<A>(Optional.empty(), 0L);

    public ACIData<A> combine(ACIData<A> a, ACIData<A> b) {
        if (a.getClock() > b.getClock()) {
            return a;
        } else {
            return b;
        }
    }

    public ACIData<A> diffFromClock(ACIData<A> a, Long from) {
        if (a.getClock() > from) {
            return a;
        } else {
            return zero;
        }
    }

    public Set<String> getRelations(ACIData<A> data) {
        if (data.getData().isPresent()) {
            return relations.apply(data.getData().get());
        } else {
            return new HashSet<>();
        }
    }

    public Long nextClock(Long current) {
        return Long.max(System.currentTimeMillis(), current + 1L);
    }
}

class ACIData<A> extends Data<Long> {
    private Optional<A> data;
    private Long clock;
    public ACIData(Optional<A> data, Long clock) {
        if (data == null) {
            this.data = Optional.empty();
        } else {
            this.data = data;
        }
        if (clock == null) {
            this.clock = 0L;
        } else {
            this.clock = clock;
        }
    }

    public Optional<A> getData() {
        return data;
    }
    public Long getClock() {
        return clock;
    }
  // TODO: implicit clockint of generic type
//  def updated(update: A): ACIData[A] = ACIData(Some(update), ACIDataOps.nextClock(clock))
}
