package ru.oseval.datahub.data.java;

public abstract class Data<C> {
    private C clock;
    public Data(C clock) {
        this.clock = clock;
    }

    public Data() {}

    public C getClock() {
        return clock;
    }
}
