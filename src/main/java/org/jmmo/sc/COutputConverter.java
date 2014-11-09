package org.jmmo.sc;

public interface COutputConverter {

    Iterable<Object> convertOut(Class<?> fieldClass, Object outputValue);
}
