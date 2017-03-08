package org.jmmo.sc;

import com.datastax.driver.core.*;
import com.datastax.driver.core.querybuilder.*;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

@SuppressWarnings("unused")
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

    public CompletableFuture<ResultSet> executeAsync(Statement statement) {
        return completableFuture(session.executeAsync(statement));
    }

    public <T> CompletableFuture<Consumer<T>> collectAsync(Consumer<T> rowConsumer, ResultSet resultSet, Function<Row, T> rowMapper) {
        IntStream.range(0, resultSet.getAvailableWithoutFetching()).forEach(i -> rowConsumer.accept(rowMapper.apply(resultSet.one())));
        return resultSet.isExhausted() ? CompletableFuture.completedFuture(rowConsumer) :
                completableFuture(resultSet.fetchMoreResults()).thenCompose(rs -> collectAsync(rowConsumer, rs, rowMapper));
    }

    public <T> Stream<T> selectAll(Statement statement, Function<Row, T> rowMapper) {
        return stream(execute(statement), rowMapper);
    }

    public <T> CompletableFuture<List<T>> selectAllAsync(Statement statement, Function<Row, T> rowMapper) {
        @SuppressWarnings("MismatchedQueryAndUpdateOfCollection")
        final List<T> result = new ArrayList<>();
        return executeAsync(statement).thenCompose(rs -> collectAsync(result::add, rs, rowMapper)).thenApply(tConsumer -> result);
    }

    public <T> Stream<T> selectAll(Class<T> entityClass) {
        final EntityInfo<T> entityInfo = entityPool.entityInfo(entityClass);
        return selectAll(entityInfo.selectQuery(Collections.emptyList()), entityInfo);
    }

    public <T> CompletableFuture<List<T>> selectAllAsync(Class<T> entityClass) {
        final EntityInfo<T> entityInfo = entityPool.entityInfo(entityClass);
        return selectAllAsync(entityInfo.selectQuery(Collections.emptyList()), entityInfo);
    }

    public <T> Stream<T> selectAll(Class<T> entityClass, Consumer<Select.Where> selectConsumer) {
        final EntityInfo<T> entityInfo = entityPool.entityInfo(entityClass);
        final Select.Where where = entityInfo.selectQuery(Collections.emptyList());
        selectConsumer.accept(where);
        return selectAll(where, entityInfo);
    }

    public <T> CompletableFuture<List<T>> selectAllAsync(Class<T> entityClass, Consumer<Select.Where> selectConsumer) {
        final EntityInfo<T> entityInfo = entityPool.entityInfo(entityClass);
        final Select.Where where = entityInfo.selectQuery(Collections.emptyList());
        selectConsumer.accept(where);
        return selectAllAsync(where, entityInfo);
    }

    public <T> Stream<T> select(Class<T> entityClass, Object key, Object... keys) {
        final EntityInfo<T> entityInfo = entityPool.entityInfo(entityClass);
        return selectAll(entityInfo.selectQuery(EntityPool.prepend(key, keys)), entityInfo);
    }

    public <T> CompletableFuture<List<T>> selectAsync(Class<T> entityClass, Object key, Object... keys) {
        final EntityInfo<T> entityInfo = entityPool.entityInfo(entityClass);
        return selectAllAsync(entityInfo.selectQuery(EntityPool.prepend(key, keys)), entityInfo);
    }

    public <T> Stream<T> select(Class<T> entityClass, Consumer<Select.Where> selectConsumer, Object key, Object... keys) {
        final EntityInfo<T> entityInfo = entityPool.entityInfo(entityClass);
        final Select.Where where = entityInfo.selectQuery(EntityPool.prepend(key, keys));
        selectConsumer.accept(where);
        return selectAll(where, entityInfo);
    }

    public <T> CompletableFuture<List<T>> selectAsync(Class<T> entityClass, Consumer<Select.Where> selectConsumer, Object key, Object... keys) {
        final EntityInfo<T> entityInfo = entityPool.entityInfo(entityClass);
        final Select.Where where = entityInfo.selectQuery(EntityPool.prepend(key, keys));
        selectConsumer.accept(where);
        return selectAllAsync(where, entityInfo);
    }

    public <T> Optional<T> selectOne(Statement statement, Function<Row, T> rowMapper) {
        return Optional.ofNullable(execute(statement).one()).map(rowMapper);
    }

    public <T> CompletableFuture<Optional<T>> selectOneAsync(Statement statement, Function<Row, T> rowMapper) {
        return executeAsync(statement).thenApply(rs -> Optional.ofNullable(rs.one()).map(rowMapper));
    }

    public <T> Optional<T> selectOne(Class<T> entityClass, Object key, Object... keys) {
        final EntityInfo<T> entityInfo = entityPool.entityInfo(entityClass);
        return selectOne(entityInfo.selectQuery(EntityPool.prepend(key, keys)), entityInfo);
    }

    public <T> CompletableFuture<Optional<T>> selectOneAsync(Class<T> entityClass, Object key, Object... keys) {
        final EntityInfo<T> entityInfo = entityPool.entityInfo(entityClass);
        return selectOneAsync(entityInfo.selectQuery(EntityPool.prepend(key, keys)), entityInfo);
    }

    public <T> Optional<T> selectOne(Class<T> entityClass, Consumer<Select.Where> selectConsumer, Object key, Object... keys) {
        final EntityInfo<T> entityInfo = entityPool.entityInfo(entityClass);
        final Select.Where where = entityInfo.selectQuery(EntityPool.prepend(key, keys));
        selectConsumer.accept(where);
        return selectOne(where, entityInfo);
    }

    public <T> CompletableFuture<Optional<T>> selectOneAsync(Class<T> entityClass, Consumer<Select.Where> selectConsumer, Object key, Object... keys) {
        final EntityInfo<T> entityInfo = entityPool.entityInfo(entityClass);
        final Select.Where where = entityInfo.selectQuery(EntityPool.prepend(key, keys));
        selectConsumer.accept(where);
        return selectOneAsync(where, entityInfo);
    }

    @SuppressWarnings("unchecked")
    public <T> T selectOne(T entity) {
        final EntityInfo<T> entityInfo = entityPool.entityInfo((Class<T>) entity.getClass());
        return selectOne(entityInfo.selectQuery(entity), entityInfo).orElse(entity);
    }

    @SuppressWarnings("unchecked")
    public <T> CompletableFuture<T> selectOneAsync(T entity) {
        final EntityInfo<T> entityInfo = entityPool.entityInfo((Class<T>) entity.getClass());
        return selectOneAsync(entityInfo.selectQuery(entity), entityInfo).thenApply(optional -> optional.orElse(entity));
    }

    public boolean isApplied(Statement statement) {
        return isApplied(execute(statement));
    }

    public CompletableFuture<Boolean> isAppliedAsync(Statement statement) {
        return executeAsync(statement).thenApply(this::isApplied);
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

    public <T> CompletableFuture<ResultSet> deleteAsync(Class<T> entityClass, Object key, Object... keys) {
        return executeAsync(entityPool.entityInfo(entityClass).deleteQuery(EntityPool.prepend(key, keys)));
    }

    public <T> ResultSet delete(Class<T> entityClass, Consumer<Delete.Where> deleteConsumer, Object key, Object... keys) {
        final Delete.Where where = entityPool.entityInfo(entityClass).deleteQuery(EntityPool.prepend(key, keys));
        deleteConsumer.accept(where);
        return execute(where);
    }

    public <T> CompletableFuture<ResultSet> deleteAsync(Class<T> entityClass, Consumer<Delete.Where> deleteConsumer, Object key, Object... keys) {
        final Delete.Where where = entityPool.entityInfo(entityClass).deleteQuery(EntityPool.prepend(key, keys));
        deleteConsumer.accept(where);
        return executeAsync(where);
    }

    @SuppressWarnings("unchecked")
    public <T> ResultSet delete(T entity) {
        return execute(entityPool.entityInfo((Class<T>) entity.getClass()).deleteQuery(entity));
    }

    @SuppressWarnings("unchecked")
    public <T> CompletableFuture<ResultSet> deleteAsync(T entity) {
        return executeAsync(entityPool.entityInfo((Class<T>) entity.getClass()).deleteQuery(entity));
    }

    @SuppressWarnings("unchecked")
    public <T> ResultSet delete(T entity, Consumer<Delete.Where> deleteConsumer) {
        final Delete.Where where = entityPool.entityInfo((Class<T>) entity.getClass()).deleteQuery(entity);
        deleteConsumer.accept(where);
        return execute(where);
    }

    @SuppressWarnings("unchecked")
    public <T> CompletableFuture<ResultSet> deleteAsync(T entity, Consumer<Delete.Where> deleteConsumer) {
        final Delete.Where where = entityPool.entityInfo((Class<T>) entity.getClass()).deleteQuery(entity);
        deleteConsumer.accept(where);
        return executeAsync(where);
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

    public <T> CompletableFuture<ResultSet> insertAsync(CEntityMapper<T> entityMapper, T entity) {
        return executeAsync(insertQuery(entityMapper, entity));
    }

    public <T> ResultSet insert(CEntityMapper<T> modelMapper, T entity, Consumer<Insert> insertConsumer) {
        final Insert insert = insertQuery(modelMapper, entity);
        insertConsumer.accept(insert);
        return execute(insert);
    }

    public <T> CompletableFuture<ResultSet> insertAsync(CEntityMapper<T> modelMapper, T entity, Consumer<Insert> insertConsumer) {
        final Insert insert = insertQuery(modelMapper, entity);
        insertConsumer.accept(insert);
        return executeAsync(insert);
    }

    public <T> Insert insertQuery(CEntityMapper<T> entityMapper, T entity) {
        return QueryBuilder.insertInto(entityMapper.table()).values(entityMapper.columns(), entityMapper.values(entity));
    }

    public <T> ResultSet insert(T entity, String... notKeyFields) {
        final Insert insert = insertQuery(entity, notKeyFields);
        return execute(insert);
    }

    public <T> CompletableFuture<ResultSet> insertAsync(T entity, String... notKeyFields) {
        final Insert insert = insertQuery(entity, notKeyFields);
        return executeAsync(insert);
    }

    public <T> ResultSet insert(T entity, Consumer<Insert> insertConsumer, String... notKeyFields) {
        final Insert insert = insertQuery(entity, notKeyFields);
        insertConsumer.accept(insert);
        return execute(insert);
    }

    public <T> CompletableFuture<ResultSet> insertAsync(T entity, Consumer<Insert> insertConsumer, String... notKeyFields) {
        final Insert insert = insertQuery(entity, notKeyFields);
        insertConsumer.accept(insert);
        return executeAsync(insert);
    }

    @SuppressWarnings("unchecked")
    public <T> Insert insertQuery(T entity, String... notKeyFields) {
        return entityPool.entityInfo((Class<T>) entity.getClass()).insertQuery(entity, notKeyFields);
    }

    public <T> ResultSet update(T entity, Assignment... assignments) {
        final Update.Where update = updateQuery(entity, assignments);
        return execute(update);
    }

    public <T> CompletableFuture<ResultSet> updateAsync(T entity, Assignment... assignments) {
        final Update.Where update = updateQuery(entity, assignments);
        return executeAsync(update);
    }

    public <T> ResultSet update(T entity, Consumer<Update.Where> insertConsumer, Assignment... assignments) {
        final Update.Where update = updateQuery(entity, assignments);
        insertConsumer.accept(update);
        return execute(update);
    }

    public <T> CompletableFuture<ResultSet> updateAsync(T entity, Consumer<Update.Where> insertConsumer, Assignment... assignments) {
        final Update.Where update = updateQuery(entity, assignments);
        insertConsumer.accept(update);
        return executeAsync(update);
    }

    @SuppressWarnings("unchecked")
    public <T> Update.Where updateQuery(T entity, Assignment... assignments) {
        return entityPool.entityInfo((Class<T>) entity.getClass()).updateQuery(entity, assignments);
    }

    public static <T> CompletableFuture<T> completableFuture(ListenableFuture<T> listenableFuture) {
        final CompletableFuture<T> completable = new CompletableFuture<T>() {
            @Override
            public boolean cancel(boolean mayInterruptIfRunning) {
                boolean result = listenableFuture.cancel(mayInterruptIfRunning);
                super.cancel(mayInterruptIfRunning);
                return result;
            }
        };

        Futures.addCallback(listenableFuture, new FutureCallback<T>() {
            @Override
            public void onSuccess(T result) {
                completable.complete(result);
            }

            @Override
            public void onFailure(Throwable t) {
                completable.completeExceptionally(t);
            }
        });

        return completable;
    }
}
