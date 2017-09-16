package org.jmmo.sc;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ParseUtils;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.mapping.annotations.ClusteringColumn;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Transient;
import org.jmmo.sc.annotation.Column;
import org.jmmo.sc.annotation.Key;
import org.jmmo.sc.annotation.Table;

import java.lang.invoke.MethodHandles;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

public class EntityPool {
    protected static final int CLUSTERING_NUMBER_OFFSET = 1000;

    protected volatile Map<Class<?>, EntityInfo> entityDataMap = new HashMap<>();
    protected final Lock lock = new ReentrantLock();
    protected final ProtocolVersion protocolVersion;
    protected final List<CInputConverter> inputConverters;
    protected final List<COutputConverter> outputConverters;

    public EntityPool() {
        this.protocolVersion = defaultProtocolVersion();
        this.inputConverters = defaultInputConverters();
        this.outputConverters = defaultOutputConverters();
    }

    public EntityPool(ProtocolVersion protocolVersion) {
        this.protocolVersion = protocolVersion;
        this.inputConverters = defaultInputConverters();
        this.outputConverters = defaultOutputConverters();
    }

    public EntityPool(ProtocolVersion protocolVersion, List<CInputConverter> inputConverters, List<COutputConverter> outputConverters) {
        this.protocolVersion = protocolVersion;
        this.inputConverters = inputConverters;
        this.outputConverters = outputConverters;
    }

    public <T> EntityInfo<T> entityInfo(Class<T> entityClass) {
        EntityInfo entityData = entityDataMap.get(entityClass);

        if (entityData == null) {
            try {
                entityData = createEntityData(entityClass);
            }
            catch (IllegalAccessException e) {
                throw new RuntimeException(e);
            }

            lock.lock();
            try {
                final EntityInfo previousData = entityDataMap.get(entityClass);
                if (previousData == null) {
                    final Map<Class<?>, EntityInfo> newMap = new HashMap<>(entityDataMap.size() + 1);
                    newMap.putAll(entityDataMap);
                    newMap.put(entityClass, entityData);
                    entityDataMap = newMap;
                }
                else {
                    entityData = previousData;
                }
            }
            finally {
                lock.unlock();
            }
        }

        //noinspection unchecked
        return entityData;
    }

    protected <T> EntityInfo<T> createEntityData(Class<T> entityClass) throws IllegalAccessException {
        final Table tableAnnotation = entityClass.getAnnotation(Table.class);
        final com.datastax.driver.mapping.annotations.Table xTableAnnotation = entityClass.getAnnotation(com.datastax.driver.mapping.annotations.Table.class);

        final String table;
        if (tableAnnotation != null && !tableAnnotation.value().isEmpty()) {
            table = tableAnnotation.value();
        }
        else if (xTableAnnotation != null) {
            table = (xTableAnnotation.keyspace().isEmpty() ? "" : xTableAnnotation.keyspace() + ".") + xTableAnnotation.name();
        }
        else {
            table = entityClass.getSimpleName().toLowerCase();
        }

        final Map<Integer, String> keys = new TreeMap<>();
        final Map<String, CFieldMapper> keyFields = new LinkedHashMap<>();
        final Map<String, CFieldMapper> regularFields = new LinkedHashMap<>();
        final Map<String, String> noQuotes2Quotes = new HashMap<>();

        for (Field field : allFields(entityClass)) {
            final int modifiers = field.getModifiers();
            if (Modifier.isStatic(modifiers)
                    || Modifier.isFinal(modifiers)
                    || Modifier.isTransient(modifiers)
                    || field.isAnnotationPresent(Transient.class)) {
                continue;
            }

            if (!field.isAccessible()) {
                field.setAccessible(true);
            }

            final Column columnAnnotation = field.getAnnotation(Column.class);
            final com.datastax.driver.mapping.annotations.Column xColumnAnnotation = field.getAnnotation(com.datastax.driver.mapping.annotations.Column.class);
            final String column;
            if (columnAnnotation != null) {
                column = columnAnnotation.value();
            }
            else if (xColumnAnnotation != null) {
                column = xColumnAnnotation.caseSensitive()
                        ? ParseUtils.doubleQuote(xColumnAnnotation.name())
                        : xColumnAnnotation.name().toLowerCase();
            }
            else {
                column = field.getName().toLowerCase();
            }

            final String noQuotesColumn = ParseUtils.unDoubleQuote(column);
            final CFieldMapper fieldMapper = new CFieldMapper(field.getType(),
                    MethodHandles.lookup().unreflectGetter(field), MethodHandles.lookup().unreflectSetter(field));

            final Key keyAnnotation = field.getAnnotation(Key.class);
            final PartitionKey partitionKeyAnnotation = field.getAnnotation(PartitionKey.class);
            final ClusteringColumn clusteringKeyAnnotation = field.getAnnotation(ClusteringColumn.class);
            if (keyAnnotation != null || partitionKeyAnnotation != null || clusteringKeyAnnotation != null) {
                final int keyNumber;
                if (keyAnnotation != null) {
                    keyNumber = keyAnnotation.value();
                }
                else if (partitionKeyAnnotation != null) {
                    keyNumber = partitionKeyAnnotation.value();
                }
                else {
                    keyNumber = clusteringKeyAnnotation.value() + CLUSTERING_NUMBER_OFFSET;
                }

                if (keys.containsKey(keyNumber)) {
                    throw new IllegalArgumentException("Key number " + keyNumber + " already exists in " + entityClass);
                }

                keys.put(keyNumber, noQuotesColumn);
                keyFields.put(noQuotesColumn, fieldMapper);
            }
            else {
                regularFields.put(noQuotesColumn, fieldMapper);
            }

            noQuotes2Quotes.put(noQuotesColumn, column);
        }

        if (keys.isEmpty()) {
            throw new IllegalArgumentException(entityClass + " has no keys");
        }

        final Map<String, CFieldMapper> fields = new LinkedHashMap<>(keyFields.size() + regularFields.size());
        for (String keyColumn : keys.values()) {
            fields.put(keyColumn, keyFields.get(keyColumn));
        }
        fields.putAll(regularFields);

        return new EntityInfo<>(entityClass, table, fields, keys, fields.keySet().stream().map(noQuotes2Quotes::get).collect(Collectors.toList()),
                inputConverters, outputConverters, protocolVersion);
    }

    @SafeVarargs
    public static <T> List<T> prepend(T firstElement, T... others) {
        if (others.length == 0) {
            return Collections.singletonList(firstElement);
        }

        final Object[] elements = new Object[others.length + 1];
        elements[0] = firstElement;
        System.arraycopy(others, 0, elements, 1, others.length);

        //noinspection unchecked
        return (List<T>) Arrays.asList(elements);
    }

    public static List<Field> allFields(Class<?> clazz) {
        final List<Field> fields = new ArrayList<>();

        do {
            fields.addAll(Arrays.asList(clazz.getDeclaredFields()));
            clazz = clazz.getSuperclass();
        } while (clazz != null);

        return fields;
    }

    public static class CInputConverterNull implements CInputConverter {

        @Override
        public Iterable<Object> convertIn(DataType rowType, Object inputValue, Class<?> fieldClass) {
            if (inputValue != null) {
                return Collections.emptyList();
            }

            if (rowType.getName() == DataType.Name.INT && fieldClass == int.class) {
                return Collections.singleton(0);
            }
            else if (rowType.getName() == DataType.Name.FLOAT && fieldClass == float.class) {
                return Collections.singleton(0f);
            }
            else if (rowType.getName() == DataType.Name.DOUBLE && fieldClass == double.class) {
                return Collections.singleton(0.0);
            }
            else if ((rowType.getName() == DataType.Name.BIGINT || rowType.getName() == DataType.Name.COUNTER) && fieldClass == long.class) {
                return Collections.singleton(0L);
            }
            else if (rowType.getName() == DataType.Name.BOOLEAN && fieldClass == boolean.class) {
                return Collections.singleton(false);
            }

            return Collections.emptyList();
        }
    }

    public static class CInputConverterBytes implements CInputConverter {

        @Override
        public Iterable<Object> convertIn(DataType rowType, Object inputValue, Class<?> fieldClass) {
            if (inputValue instanceof ByteBuffer && fieldClass == byte[].class) {
                return Collections.singleton(byteArray((ByteBuffer) inputValue));
            }

            return Collections.emptyList();
        }

        public static byte[] byteArray(ByteBuffer buffer) {
            if (buffer.hasArray() && buffer.arrayOffset() == 0 && buffer.array().length == buffer.remaining()) {
                return buffer.array();
            }

            final byte[] result = new byte[buffer.remaining()];
            buffer.get(result);

            return result;
        }
    }

    public static class COutputConverterBytes implements COutputConverter {

        @Override
        public Iterable<Object> convertOut(Class<?> fieldClass, Object outputValue) {
            if (outputValue instanceof byte[]) {
                return Collections.singleton(ByteBuffer.wrap((byte[]) outputValue));
            }

            return Collections.emptyList();
        }
    }

    public static class CInputConverterEnum implements CInputConverter {

        @Override
        public Iterable<Object> convertIn(DataType rowType, Object inputValue, Class<?> fieldClass) {
            if (inputValue instanceof Number && fieldClass.isEnum()) {
                return Collections.singleton(fieldClass.getEnumConstants()[((Number) inputValue).intValue()]);
            }

            return Collections.emptyList();
        }
    }

    public static class COutputConverterEnum implements COutputConverter {

        @Override
        public Iterable<Object> convertOut(Class<?> fieldClass, Object outputValue) {
            if (outputValue instanceof Enum) {
                return Collections.singleton(((Enum) outputValue).ordinal());
            }

            return Collections.emptyList();
        }
    }

    protected ProtocolVersion defaultProtocolVersion() {
        return ProtocolVersion.NEWEST_SUPPORTED;
    }

    protected List<CInputConverter> defaultInputConverters() {
        final List<CInputConverter> converters = new ArrayList<>();

        converters.add(new CInputConverterNull());
        converters.add(new CInputConverterBytes());
        converters.add(new CInputConverterEnum());

        return converters;
    }

    protected List<COutputConverter> defaultOutputConverters() {
        final List<COutputConverter> converters = new ArrayList<>();

        converters.add(new COutputConverterBytes());
        converters.add(new COutputConverterEnum());

        return converters;
    }
}
