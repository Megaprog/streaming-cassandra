package org.jmmo.sc;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.Assignment;
import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.querybuilder.Update;

import java.util.Collections;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class Cassandra {
    private final Session session;
    private final EntityPool entityPool;

    public Cassandra(Session session) {
        this(session, new EntityPool());
    }

    public Cassandra(Session session, EntityPool entityPool) {
        this.session = session;
        this.entityPool = entityPool;
    }

    public Session getSession() {
        return session;
    }

    public EntityPool getEntityPoll() {
        return entityPool;
    }

    public Stream<Row> stream(ResultSet resultSet) {
        return StreamSupport.stream(resultSet.spliterator(), false);
    }

    public <T> Stream<T> stream(ResultSet resultSet, Function<Row, T> rowMapper) {
        return StreamSupport.stream(resultSet.spliterator(), false).map(rowMapper::apply);
    }

    public ResultSet execute(String query) {
        return execute(new SimpleStatement(query));
    }

    public ResultSet execute(Statement statement) {
        return getSession().execute(statement);
    }

    public ResultSetFuture executeAsync(Statement statement) {
        return session.executeAsync(statement);
    }

    public <T> Stream<T> selectAll(Statement statement, Function<Row, T> rowMapper) {
        return stream(execute(statement), rowMapper);
    }

    public <T> Stream<T> selectAll(Class<T> entityClass) {
        final EntityInfo<T> entityInfo = entityPool.entityInfo(entityClass);
        return selectAll(entityInfo.selectQuery(Collections.emptyList()), entityInfo);
    }

    public <T> Stream<T> selectAll(Class<T> entityClass, Consumer<Select.Where> selectConsumer) {
        final EntityInfo<T> entityInfo = entityPool.entityInfo(entityClass);
        final Select.Where where = entityInfo.selectQuery(Collections.emptyList());
        selectConsumer.accept(where);
        return selectAll(where, entityInfo);
    }

    public <T> Stream<T> select(Class<T> entityClass, Object key, Object... keys) {
        final EntityInfo<T> entityInfo = entityPool.entityInfo(entityClass);
        return selectAll(entityInfo.selectQuery(EntityPool.prepend(key, keys)), entityInfo);
    }

    public <T> Stream<T> select(Class<T> entityClass, Consumer<Select.Where> selectConsumer, Object key, Object... keys) {
        final EntityInfo<T> entityInfo = entityPool.entityInfo(entityClass);
        final Select.Where where = entityInfo.selectQuery(EntityPool.prepend(key, keys));
        selectConsumer.accept(where);
        return selectAll(where, entityInfo);
    }

    public <T> Optional<T> selectOne(Statement statement, Function<Row, T> rowMapper) {
        final ResultSet resultSet = execute(statement);
        if (resultSet.isExhausted()) {
            return Optional.empty();
        }
        else {
            return Optional.of(rowMapper.apply(resultSet.one()));
        }
    }

    public <T> Optional<T> selectOne(Class<T> entityClass, Object key, Object... keys) {
        final EntityInfo<T> entityInfo = entityPool.entityInfo(entityClass);
        return selectOne(entityInfo.selectQuery(EntityPool.prepend(key, keys)), entityInfo);
    }

    public <T> Optional<T> selectOne(Class<T> entityClass, Consumer<Select.Where> selectConsumer, Object key, Object... keys) {
        final EntityInfo<T> entityInfo = entityPool.entityInfo(entityClass);
        final Select.Where where = entityInfo.selectQuery(EntityPool.prepend(key, keys));
        selectConsumer.accept(where);
        return selectOne(where, entityInfo);
    }

    @SuppressWarnings("unchecked")
    public <T> T selectOne(T entity) {
        final EntityInfo<T> entityInfo = entityPool.entityInfo((Class<T>) entity.getClass());
        return selectOne(entityInfo.selectQuery(entity), entityInfo).orElse(entity);
    }

    public boolean isApplied(Statement statement) {
        return isApplied(execute(statement));
    }

    public boolean isApplied(ResultSet resultSet) {
        return isApplied(resultSet.one());
    }

    public boolean isApplied(Row row) {
        return row.getBool(0);
    }

    public <T> ResultSet delete(Class<T> entityClass, Object key, Object... keys) {
        return execute(entityPool.entityInfo(entityClass).deleteQuery(EntityPool.prepend(key, keys)));
    }

    public <T> ResultSet delete(Class<T> entityClass, Consumer<Delete.Where> deleteConsumer, Object key, Object... keys) {
        final Delete.Where where = entityPool.entityInfo(entityClass).deleteQuery(EntityPool.prepend(key, keys));
        deleteConsumer.accept(where);
        return execute(where);
    }

    @SuppressWarnings("unchecked")
    public <T> ResultSet delete(T entity) {
        return execute(entityPool.entityInfo((Class<T>) entity.getClass()).deleteQuery(entity));
    }

    @SuppressWarnings("unchecked")
    public <T> ResultSet delete(T entity, Consumer<Delete.Where> deleteConsumer) {
        final Delete.Where where = entityPool.entityInfo((Class<T>) entity.getClass()).deleteQuery(entity);
        deleteConsumer.accept(where);
        return execute(where);
    }

    @SuppressWarnings("unchecked")
    public <T> Delete.Where deleteQuery(T entity) {
        return entityPool.entityInfo((Class<T>) entity.getClass()).deleteQuery(entity);
    }

    public <T> Delete.Where deleteQuery(Class<T> entityClass, Object key, Object... keys) {
        return entityPool.entityInfo(entityClass).deleteQuery(EntityPool.prepend(key, keys));
    }

    public <T> ResultSet insert(CEntityMapper<T> entityMapper, T entity) {
        return execute(insertQuery(entityMapper, entity));
    }

    public <T> ResultSet insert(CEntityMapper<T> modelMapper, T entity, Consumer<Insert> insertConsumer) {
        final Insert insert = insertQuery(modelMapper, entity);
        insertConsumer.accept(insert);
        return execute(insert);
    }

    public <T> Insert insertQuery(CEntityMapper<T> entityMapper, T entity) {
        return QueryBuilder.insertInto(entityMapper.table()).values(entityMapper.columns(), entityMapper.values(entity));
    }

    public <T> ResultSet insert(T entity, String... notKeyFields) {
        final Insert insert = insertQuery(entity, notKeyFields);
        return execute(insert);
    }

    public <T> ResultSet insert(T entity, Consumer<Insert> insertConsumer, String... notKeyFields) {
        final Insert insert = insertQuery(entity, notKeyFields);
        insertConsumer.accept(insert);
        return execute(insert);
    }

    @SuppressWarnings("unchecked")
    public <T> Insert insertQuery(T entity, String... notKeyFields) {
        return entityPool.entityInfo((Class<T>) entity.getClass()).insertQuery(entity, notKeyFields);
    }

    public <T> ResultSet update(T entity, Assignment... assignments) {
        final Update.Where update = updateQuery(entity, assignments);
        return execute(update);
    }

    public <T> ResultSet update(T entity, Consumer<Update.Where> insertConsumer, Assignment... assignments) {
        final Update.Where update = updateQuery(entity, assignments);
        insertConsumer.accept(update);
        return execute(update);
    }

    @SuppressWarnings("unchecked")
    public <T> Update.Where updateQuery(T entity, Assignment... assignments) {
        return entityPool.entityInfo((Class<T>) entity.getClass()).updateQuery(entity, assignments);
    }
}
