package org.jmmo.sc.consumer;

import com.datastax.driver.core.querybuilder.Ordering;
import com.datastax.driver.core.querybuilder.Select;

import java.util.Arrays;
import java.util.function.Consumer;

public class OrderBy implements Consumer<Select.Where> {
    private final Ordering[] orderings;

    public OrderBy(Ordering... orderings) {
        this.orderings = orderings;
    }

    @Override
    public void accept(Select.Where where) {
        where.orderBy(orderings);
    }

    @Override
    public String toString() {
        return "OrderBy{" +
                "orderings=" + Arrays.toString(orderings) +
                '}';
    }
}
