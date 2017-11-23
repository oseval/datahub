package ru.oseval.datahab.data.java;

public abstract class AtLeastOnceData<C> extends Data<C> {
    C previousClock;
    boolean isSolid;
}
