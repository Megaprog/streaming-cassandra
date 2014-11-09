package org.jmmo.sc.consumer;

import com.datastax.driver.core.querybuilder.Clause;
import com.datastax.driver.core.querybuilder.Select;

import java.util.Arrays;
import java.util.function.Consumer;

public class Clauses implements Consumer<Select.Where> {
    private final Clause[] clauses;

    public Clauses(Clause... clauses) {
        this.clauses = clauses;
    }

    @Override
    public void accept(Select.Where where) {
        for (Clause clause : clauses) {
            where.and(clause);
        }
    }

    @Override
    public String toString() {
        return "Clauses{" +
                "clauses=" + Arrays.toString(clauses) +
                '}';
    }
}
