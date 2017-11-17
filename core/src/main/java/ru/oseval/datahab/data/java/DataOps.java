package ru.oseval.datahab.data.java;

import com.sun.istack.internal.NotNull;

import java.util.Comparator;
import java.util.Optional;
import java.util.Set;

public abstract class DataOps {
    public abstract class D extends Data {}

    @NotNull
    public Comparator<D.C> ordering;
    /**
     * Data which is initial state for all such entities
     */
    @NotNull
    public D zero;

    /**
     * Combines two data objects to one
     * @param a
     * @param b
     * @return
     */
    @NotNull
    public abstract D combine(D a, D b);

    /**
     * Computes diff between `a` and older state with a `from` id
     * @param a
     * @param from
     * @return
     */
    @NotNull
    public abstract D diffFromClock(D a, D.C from);

    @NotNull
    public abstract D.C nextClock(D.C current);

    @NotNull
    public Optional<D> matchData(Data data) {
        if (zero.getClass().isAssignableFrom(data.getClass())) {
            return Optional.of(zero.getClass().cast(data));
        } else {
            return Optional.empty();
        }
    }

    @NotNull
    public Optional<D.C> matchClock(Object clock) {
        if (clock.getClass() == zero.clock.getClass()) {
            return Optional.of((D.C) clock);
        } else {
            return Optional.empty();
        }
    }

    @NotNull
    public abstract Set<String> getRelations(D data);
}
