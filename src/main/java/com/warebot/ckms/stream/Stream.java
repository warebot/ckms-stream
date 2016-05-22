package com.warebot.ckms.stream;

import java.util.List;
import java.util.Map;

public interface Stream<T extends Number & Comparable<T>> {
    void insert(T v);

    void merge(List<T> buffer);

    void reset();

    T query(double quantile) throws IllegalStateException;

    Map<Quantile, T> getSnapshot(Quantile... quantiles);

    List<T> getSamples();
}
