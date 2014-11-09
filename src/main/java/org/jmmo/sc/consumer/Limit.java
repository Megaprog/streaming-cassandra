package org.jmmo.sc.consumer;

import com.datastax.driver.core.querybuilder.Select;

import java.util.function.Consumer;

public class Limit implements Consumer<Select.Where> {
    private final int limit;

    public Limit(int limit) {
        this.limit = limit;
    }

    @Override
    public void accept(Select.Where where) {
        where.limit(limit);
    }

    @Override
    public String toString() {
        return "Limit{" +
                "limit=" + limit +
                '}';
    }
}
