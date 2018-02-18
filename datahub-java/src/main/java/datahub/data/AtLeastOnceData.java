package ru.oseval.datahub.data.j;

public abstract class AtLeastOnceData<C> extends Data<Long> {
    C previousClock;
    boolean isSolid;
}
