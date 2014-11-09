package org.jmmo.sc.consumer;

import com.datastax.driver.core.querybuilder.Insert;

import java.util.function.Consumer;

public class IfNotExists implements Consumer<Insert> {
    @Override
    public void accept(Insert insert) {
        insert.ifNotExists();
    }

    @Override
    public String toString() {
        return "IfNotExists{}";
    }
}
