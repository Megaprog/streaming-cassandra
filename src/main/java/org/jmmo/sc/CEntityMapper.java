package org.jmmo.sc;

public interface CEntityMapper<T> {

    String table();

    String[] columns();

    Object[] values(T t);
}
