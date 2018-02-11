package ru.oseval.datahub.data.java;

import com.sun.istack.internal.Nullable;

import java.util.*;
import java.util.function.Function;

abstract class ALODataOps<A> extends DataOps<Long> {
    private Function<A, Set<String>> relations;
    public ALODataOps(Function<A, Set<String>> relations) {
        if (relations == null) {
            this.relations = x -> { return new HashSet<>(); };
        } else {
            this.relations = relations;
        }
    }

    public static Comparator<Long> ordering = Comparator.naturalOrder();
    public static ALOData<?> zero = new ALOData<>(null, null,null);

    public Set<String> getRelations(ALOData<A> data) {
        Collection<A> elems = data.elements();
        Set<String> rels = new HashSet<>();

        for (A el : elems) {
            rels.addAll(relations.apply(el));
        }

        return rels;
    }

    public ALOData<A> diffFromClock(ALOData<A> a, Long from) {
        return new ALOData<A>(a.getData().tailMap(from), a.getClock(), from, a.getFurther());
    }

    public Long nextClock(Long current) {
        return Math.max(System.currentTimeMillis(), current + 1L);
    }

    public ALOData<A> combine(ALOData<A> a, ALOData<A> b) {
        ALOData<A> first = a;
        ALOData<A> second = b;

        if (a.getClock() > b.getClock()) {
            first = b;
            second = a;
        }

//      | --- | |---|
//
//      | --- |
//         | --- |
//
//        | --- |
//      | -------- |

        ALOData result;

        if (first.getClock() >= second.getPreviousClock()) {
            if (first.getPreviousClock() >= second.getPreviousClock()) {
                result = second;
            } else {
                SortedMap<Long, A> totalData = new TreeMap<>(ordering);
                totalData.putAll(second.getData());
                totalData.putAll(first.getData());
                ALOData<A> visible = new ALOData<A>(totalData, second.getClock(), first.getPreviousClock());

                ALOData<A> further =
                        first.getFurther() == null ?
                                second.getFurther() :
                                (second.getFurther() == null ?
                                        first.getFurther() :
                                        combine(first.getFurther(), second.getFurther())
                                );

                result = further != null ? combine(visible, further) : visible;
            }
        } else {// further
            SortedMap<Long, A> totalData = new TreeMap<>(ordering);
            totalData.putAll(second.getData());
            totalData.putAll(first.getData());
            result = new ALOData(
                    totalData,
                    first.getClock(),
                    first.getPreviousClock(),
                    first.getFurther() != null ? combine(first.getFurther(), second) : second
            );
        }

        return result;
  }
}

class ALOData<A> extends AtLeastOnceData<A> {
    private SortedMap<Long, A> data;
    private Long clock;
    private Long previousClock;
    @Nullable private ALOData<A> further;

    public ALOData(SortedMap<Long, A> data,
                   Long clock,
                   Long previousClock,
                   @Nullable ALOData<A> further) {
        this.data = data != null ? data : new TreeMap<Long, A>(ALODataOps.ordering);
        this.clock = clock != null ? clock : System.currentTimeMillis();
        this.previousClock = previousClock != null ? previousClock : 0L;
        this.further = further;
    }

    public ALOData(SortedMap<Long, A> data,
                   Long clock,
                   Long previousClock) {
        this.data = data != null ? data : new TreeMap<Long, A>(ALODataOps.ordering);
        this.clock = clock != null ? clock : System.currentTimeMillis();
        this.previousClock = previousClock != null ? previousClock : 0L;
    }

    public boolean isSolid() {
        return further == null;
    }

    public Collection<A> elements() {
        return data.values();
    }

    public ALOData<A> updated(A update, Long newClock) {
        SortedMap<Long, A> newData = new TreeMap<>(ALODataOps.ordering);
        newData.putAll(data);
        newData.put(newClock, update);

        return new ALOData<>(newData, newClock, clock);
    }

    SortedMap<Long, A> getData() {
        return data;
    }

    public Long getClock() {
        return clock;
    }

    public Long getPreviousClock() {
        return previousClock;
    }

    @Nullable ALOData<A> getFurther() {
        return further;
    }
}