package ru.oseval.datahub.data.java;

import com.sun.istack.internal.NotNull;

import java.util.Comparator;
import java.util.Optional;
import java.util.Set;

public abstract class DataOps<C> {
    public abstract class D extends Data<C> {};

    protected String kind = getClass().getName();

    @NotNull
    private Comparator<C> ordering;
    /**
     * Data which is initial state for all such entities
     */
    @NotNull
    private D zero;

    public Comparator<C> getOrdering() {
        return ordering;
    }

    public D getZero() {
        return zero;
    }

    public String getKind() {
        return kind;
    }

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
    public abstract D diffFromClock(D a, C from);

    @NotNull
    public abstract C nextClock(C current);

    @NotNull
    public Optional<D> matchData(Data data) {
        if (zero.getClass().isAssignableFrom(data.getClass())) {
            return Optional.of(zero.getClass().cast(data));
        } else {
            return Optional.empty();
        }
    }

    @NotNull
    public Optional<C> matchClock(Object clock) {
        if (clock.getClass() == zero.getClock().getClass()) {
            return Optional.of((C) clock);
        } else {
            return Optional.empty();
        }
    }

    @NotNull
    public abstract Set<String> getRelations(D data);
}
