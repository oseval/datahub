package ru.oseval.datahub.data.java;

public abstract class AtLeastOnceData<C> extends Data<Long> {
    C previousClock;
    boolean isSolid;
}
