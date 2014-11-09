package org.jmmo.sc.consumer;

import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Update;

import java.util.function.Consumer;

public class UpdateTtl implements Consumer<Update.Where> {
    private final int ttl;

    public UpdateTtl(int ttl) {
        this.ttl = ttl;
    }

    @Override
    public void accept(Update.Where where) {
        where.using(QueryBuilder.ttl(ttl));
    }

    @Override
    public String toString() {
        return "UpdateTtl{" +
                "ttl=" + ttl +
                '}';
    }
}
