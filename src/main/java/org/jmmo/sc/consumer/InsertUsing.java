package org.jmmo.sc.consumer;

import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.Using;

import java.util.function.Consumer;

public class InsertUsing implements Consumer<Insert> {
    private final Using using;

    public InsertUsing(Using using) {
        this.using = using;
    }

    @Override
    public void accept(Insert insert) {
        insert.using(using);
    }

    @Override
    public String toString() {
        return "InsertUsing{" +
                "using=" + using +
                '}';
    }
}
