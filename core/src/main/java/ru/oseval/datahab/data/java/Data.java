package ru.oseval.datahab.data.java;

public abstract class Data {
    abstract class Cl<C> {}
    abstract class T<C> extends Cl<T<C>> {}

    Cl<?> x;

    public Cl<? extends x> clock;
}
