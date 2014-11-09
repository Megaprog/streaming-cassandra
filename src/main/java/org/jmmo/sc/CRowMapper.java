package org.jmmo.sc;

import com.datastax.driver.core.Row;

import java.util.function.Function;

public interface CRowMapper<T> extends Function<Row, T> {
}
