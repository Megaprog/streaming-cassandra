package org.jmmo.sc;

import com.datastax.driver.core.DataType;

public interface CInputConverter {

    Iterable<Object> convertIn(DataType rowType, Object inputValue, Class<?> fieldClass);
}
