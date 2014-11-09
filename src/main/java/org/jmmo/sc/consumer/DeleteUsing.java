package org.jmmo.sc.consumer;

import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.Using;

import java.util.function.Consumer;

public class DeleteUsing implements Consumer<Delete.Where> {
    private final Using using;

    public DeleteUsing(Using using) {
        this.using = using;
    }

    @Override
    public void accept(Delete.Where where) {
        where.using(using);
    }

    @Override
    public String toString() {
        return "DeleteUsing{" +
                "using=" + using +
                '}';
    }
}
