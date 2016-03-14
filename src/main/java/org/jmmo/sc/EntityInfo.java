package org.jmmo.sc;

import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.querybuilder.Assignment;
import com.datastax.driver.core.querybuilder.Clause;
import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.querybuilder.Update;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class EntityInfo<T> implements CMapper<T> {
    protected final Class<T> entityClass;
    protected final String table;
    protected final Map<String, CFieldMapper> fields;
    protected final Map<Integer, String> keys;
    protected final String[] columns;
    protected final List<CInputConverter> inputConverters;
    protected final List<COutputConverter> outputConverters;
    protected final ProtocolVersion protocolVersion;

    public EntityInfo(Class<T> entityClass, String table, Map<String, CFieldMapper> fields, Map<Integer, String> keys, List<String> columns,
                       List<CInputConverter> inputConverters, List<COutputConverter> outputConverters, ProtocolVersion protocolVersion) {
        this.entityClass = entityClass;
        this.table = table;
        this.protocolVersion = protocolVersion;
        this.fields = fields;
        this.keys = keys;
        this.columns = columns.toArray(new String[columns.size()]);
        this.inputConverters = inputConverters;
        this.outputConverters = outputConverters;
    }

    public Select.Where selectQuery(List<Object> keyValues) {
        final Select.Where where = QueryBuilder.select(columns()).from(table()).where();

        createClauses(checkKeyValues(keyValues)).forEach(where::and);

        return where;
    }

    public Select.Where selectQuery(T entity) {
        return selectQuery(keyValues(entity));
    }

    public Delete.Where deleteQuery(List<Object> keyValues) {
        final Delete.Where where = QueryBuilder.delete().from(table()).where();

        createClauses(checkKeyValues(keyValues)).forEach(where::and);

        return where;
    }

    public Delete.Where deleteQuery(T entity) {
        return deleteQuery(keyValues(entity));
    }

    public Insert insertQuery(T entity) {
        return QueryBuilder.insertInto(table()).values(columns(), values(entity));
    }

    public Insert insertQuery(T entity, String... notKeyFields) {
        final Insert insert = QueryBuilder.insertInto(table());

        final List<Object> keyValues = keyValues(entity);
        for (int i = 0; i < keyValues.size(); i++) {
            insert.value(columns()[i], keyValues.get(i));
        }

        if (notKeyFields.length == 0) {
            int columnIndex = 0;
            for (CFieldMapper fieldMapper : fields.values()) {
                if (columnIndex >= keys.size()) {
                    insert.value(columns()[columnIndex], fieldValue(entity, fieldMapper));
                }

                columnIndex++;
            }
        }
        else {
            for (String fieldName : notKeyFields) {
                final String noQuotesName = Quotes.removeQuotes(fieldName);
                final CFieldMapper fieldMapper = fields.get(noQuotesName);
                if (fieldMapper == null) {
                    throw new IllegalArgumentException("There is wrong field name was specified");
                }
                if (keys.values().contains(noQuotesName)) {
                    continue;
                }

                insert.value(fieldName, fieldValue(entity, fieldMapper));
            }
        }

        return insert;
    }

    public Update.Where updateQuery(T entity, Assignment...assignments) {
        final Update.Assignments ass = QueryBuilder.update(table()).with();

        if (assignments.length == 0) {
            int columnIndex = 0;
            for (CFieldMapper fieldMapper : fields.values()) {
                if (columnIndex >= keys.size()) {
                    ass.and(QueryBuilder.set(columns()[columnIndex], fieldValue(entity, fieldMapper)));
                }

                columnIndex++;
            }
        }
        else {
            for (Assignment assignment : assignments) {
                ass.and(assignment);
            }
        }

        final List<Clause> clauses = createClauses(keyValues(entity));
        final Update.Where where = ass.where(clauses.get(0));
        for (int i = 1; i < clauses.size(); i++) {
            where.and(clauses.get(i));
        }

        return where;
    }

    protected List<Object> keyValues(T entity) {
        return keys.values().stream().map(keyName -> fieldValue(entity, fields.get(keyName))).collect(Collectors.toList());
    }

    protected Object fieldValue(T entity, CFieldMapper fieldMapper) {
        final Object value;
        try {
            value = fieldMapper.getGetter().invoke(entity);
        }
        catch (Throwable throwable) {
            throw new RuntimeException(throwable);
        }

        return outputConverters.stream().map(converter -> converter.convertOut(fieldMapper.getFieldClass(), value))
                .map(Iterable::iterator).filter(Iterator::hasNext).map(Iterator::next).findFirst().orElse(value);
    }

    protected List<Object> checkKeyValues(List<Object> keyValues) {
        if (keyValues.size() > keys.size()) {
            throw new IllegalArgumentException("Specified " + keyValues.size() + " key values but " + table() + " has only " + keys.size() + " keys");
        }

        return keyValues;
    }

    protected List<Clause> createClauses(List<Object> keyValues) {
        final List<Clause> clauses = new ArrayList<>(keyValues.size());

        for (int i = 0; i < keyValues.size(); i++) {
            clauses.add(QueryBuilder.eq(columns()[i], keyValues.get(i)));
        }

        return clauses;
    }

    @Override
    public T apply(Row row) {
        final T entity;
        try {
            entity = entityClass.newInstance();
        }
        catch (InstantiationException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }

        for (ColumnDefinitions.Definition definition : row.getColumnDefinitions()) {
            final CFieldMapper fieldMapper = fields.get(definition.getName());
            if (fieldMapper == null) {
                continue;
            }

            final Object value = row.isNull(definition.getName()) ? null : CodecRegistry.DEFAULT_INSTANCE.codecFor(definition.getType())
                    .deserialize(row.getBytesUnsafe(definition.getName()), protocolVersion);

            try {
                fieldMapper.getSetter().invoke(entity, inputConverters.stream().map(converter -> converter.convertIn(definition.getType(), value, fieldMapper.getFieldClass()))
                        .map(Iterable::iterator).filter(Iterator::hasNext).map(Iterator::next).findFirst().orElse(value));
            }
            catch (Throwable throwable) {
                throw new RuntimeException(throwable);
            }
        }

        return entity;
    }

    @Override
    public String table() {
        return table;
    }

    @Override
    public String[] columns() {
        return columns;
    }

    @Override
    public Object[] values(T t) {
        final Object[] values = new Object[fields.size()];

        int index = 0;
        for (CFieldMapper fieldMapper : fields.values()) {
            values[index++] = fieldValue(t, fieldMapper);
        }

        return values;
    }

    @Override
    public String toString() {
        return "EntityInfo{" +
                "entityClass=" + entityClass.getName() +
                ", table='" + table + '\'' +
                ", fields=" + fields +
                ", keys=" + keys +
                ", columns=" + Arrays.toString(columns) +
                '}';
    }
}
