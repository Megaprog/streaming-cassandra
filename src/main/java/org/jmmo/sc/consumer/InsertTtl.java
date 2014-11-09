package org.jmmo.sc.consumer;

import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;

import java.util.function.Consumer;

public class InsertTtl implements Consumer<Insert> {
    private final int ttl;

    public InsertTtl(int ttl) {
        this.ttl = ttl;
    }

    @Override
    public void accept(Insert insert) {
        insert.using(QueryBuilder.ttl(ttl));
    }

    @Override
    public String toString() {
        return "InsertTtl{" +
                "ttl=" + ttl +
                '}';
    }
}
