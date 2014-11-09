package org.jmmo.sc;

import java.lang.invoke.MethodHandle;

public class CFieldMapper {
    private final Class<?> fieldClass;
    private final MethodHandle getter;
    private final MethodHandle setter;

    public CFieldMapper(Class<?> fieldClass, MethodHandle getter, MethodHandle setter) {
        this.fieldClass = fieldClass;
        this.getter = getter;
        this.setter = setter;
    }

    public Class<?> getFieldClass() {
        return fieldClass;
    }

    public MethodHandle getGetter() {
        return getter;
    }

    public MethodHandle getSetter() {
        return setter;
    }

    @Override
    public String toString() {
        return "FieldMapper{" +
                "fieldClass=" + fieldClass.getName() +
                ", getter=" + getter +
                ", setter=" + setter +
                '}';
    }
}
