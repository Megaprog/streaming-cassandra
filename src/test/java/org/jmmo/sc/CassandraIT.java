package org.jmmo.sc;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import org.apache.cassandra.service.EmbeddedCassandraService;
import org.hamcrest.Matchers;
import org.jmmo.sc.annotation.Column;
import org.jmmo.sc.annotation.Key;
import org.jmmo.sc.annotation.Table;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.lang.annotation.RetentionPolicy;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class CassandraIT {
    private static final String KEYSPACE = "test";

    private static EntityPool entityPool = new EntityPool();
    private static EntityInfo<Example> entityInfo = entityPool.entityInfo(Example.class);
    private static Cassandra cassandra;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        new EmbeddedCassandraService().start();

        cassandra = new Cassandra(Cluster.builder().addContactPoint("localhost").build().connect(), entityPool);

        cassandra.execute("CREATE KEYSPACE IF NOT EXISTS " + KEYSPACE + " WITH replication = {'class':'SimpleStrategy', 'replication_factor':1};");
        cassandra.execute("USE " + KEYSPACE);

        cassandra.execute("CREATE TABLE IF NOT EXISTS " + entityInfo.table() + " (\n" +
                "  user_rank int,\n" +
                "  user_id uuid,\n" +
                "  \"user_Name\" text,\n" +
                "  data blob,\n" +
                "  number int,\n" +
                "  enable boolean,\n" +
                "  retention_policy int,\n" +
                "  PRIMARY KEY (user_rank, user_id)\n" +
                ")");
        cassandra.execute("truncate " + entityInfo.table());
    }

    @Test(expected = IllegalArgumentException.class)
    public void test01_Exception_NoColumns() throws Exception {
        entityPool.entityInfo(Object.class);
    }

    private static class NoKeys {
        int value;
    }

    @Test(expected = IllegalArgumentException.class)
    public void test02_Exception_NoKeys() throws Exception {
        entityPool.entityInfo(NoKeys.class);
    }

    private static class EqualKeys {
        @Key(1)
        int key1;
        @Key(1)
        int key2;
    }

    @Test(expected = IllegalArgumentException.class)
    public void test03_Exception_EqualKeys() throws Exception {
        entityPool.entityInfo(EqualKeys.class);
    }

    @Test
    public void test04_Structure() throws Exception {
        assertEquals(Example.class, entityInfo.entityClass);
        assertEquals("\"test_streaming_Cassandra\"", entityInfo.table());

        assertEquals(7, entityInfo.fields.size());
        assertThat(entityInfo.fields.keySet(), containsInAnyOrder("user_rank", "user_id", "user_Name", "data", "number", "enable", "retention_policy"));

        assertThat(entityInfo.columns(), arrayContainingInAnyOrder("user_rank", "user_id", "\"user_Name\"", "data", "number", "enable", "retention_policy"));

        assertEquals(2, entityInfo.keys.size());
        assertThat(entityInfo.keys, allOf(hasEntry(1, "user_rank"), hasEntry(2, "user_id")));
    }

    private static UUID userId = UUID.randomUUID();
    private static String userName = "Admin";
    private static byte[] data = {5, 6, 7};
    private static ByteBuffer buffer = ByteBuffer.wrap(data);
    private static Example example = new Example(1, userId, userName, data, 2, true, RetentionPolicy.RUNTIME);

    @Test
    public void test005_ToValues() throws Exception {
        Object[] values = entityInfo.values(example);
        assertThat(values, allOf(arrayWithSize(7), Matchers.arrayContainingInAnyOrder(1, userId, userName, buffer, 2, true, RetentionPolicy.RUNTIME.ordinal())));

        //check order
        Map<String, Integer> columnsMap = new HashMap<>();
        for (int i = 0; i < entityInfo.columns().length; i++) {
            columnsMap.put(entityInfo.columns()[i], i);
        }
        assertEquals(1, values[columnsMap.get("user_rank")]);
        assertEquals(userId, values[columnsMap.get("user_id")]);
        assertEquals(userName, values[columnsMap.get("\"user_Name\"")]);
        assertEquals(buffer, values[columnsMap.get("data")]);
        assertEquals(2, values[columnsMap.get("number")]);
        assertEquals(true, values[columnsMap.get("enable")]);
        assertEquals(RetentionPolicy.RUNTIME.ordinal(), values[columnsMap.get("retention_policy")]);

        assertThat(entityInfo.values(new Example(1, userId, userName)),
                allOf(arrayWithSize(7), Matchers.arrayContainingInAnyOrder(1, userId, userName, null, 0, false, null)));
    }

    @Test
    public void test11_Insert() throws Exception {
        cassandra.insert(example);
        assertThat(iterable(cassandra.select(Example.class, 1)), contains(example));

        assertEquals(1, entityPool.entityDataMap.size());
    }

    private static Example example1 = new Example(1, UUID.randomUUID(), "Other");

    @Test
    public void test12_Selects() throws Exception {
        cassandra.insert(example1);
        assertThat(iterable(cassandra.select(Example.class, 1)), containsInAnyOrder(example, example1));
        assertThat(iterable(cassandra.select(Example.class, 1, userId)), contains(example));

        assertEquals(example, cassandra.selectOne(Example.class, 1, example.getUserId()).get());
        assertEquals(example1, cassandra.selectOne(Example.class, 1, example1.getUserId()).get());

        assertFalse(cassandra.selectOne(Example.class, 1, UUID.randomUUID()).isPresent());

        Example newExample = new Example(2, UUID.randomUUID());
        assertEquals(newExample, cassandra.selectOne(newExample));
    }

    @Test(expected = IllegalArgumentException.class)
    public void test13_SelectTooManyKeys() throws Exception {
        cassandra.select(Example.class, 1, UUID.randomUUID(), 3);
    }

    @Test
    public void test14_SelectNulls() throws Exception {
        UUID id = UUID.randomUUID();
        cassandra.execute(QueryBuilder.insertInto(entityInfo.table()).values(new String[]{"user_rank", "user_id"}, new Object[]{1, id}));
        Example other = cassandra.selectOne(Example.class, 1, id).get();
        assertEquals(0, other.getNumber());
        assertEquals(false, other.isEnable());
    }

    @Test
    public void test15_InsertSomeFields() throws Exception {
        UUID id = UUID.randomUUID();
        cassandra.insert(new Example(1, id, "Mike", new byte[]{3, 2, 1}, 5, true, RetentionPolicy.CLASS), "\"user_Name\"", "data");

        Example other = cassandra.selectOne(Example.class, 1, id).get();
        assertEquals("Mike", other.getUserName());
        assertArrayEquals(new byte[]{3, 2, 1}, other.getData());
        assertEquals(0, other.getNumber());
        assertEquals(false, other.isEnable());
    }

    @Test
    public void test16_Update() throws Exception {
        UUID id = UUID.randomUUID();
        final Example updatedEntity = new Example(1, id, "Jon", new byte[]{1, 2, 3}, 5, true, RetentionPolicy.CLASS);
        cassandra.update(updatedEntity);

        assertEquals(updatedEntity, cassandra.selectOne(Example.class, 1, id).get());
    }

    @Test
    public void test17_UpdateSomeFields() throws Exception {
        UUID id = UUID.randomUUID();
        cassandra.update(new Example(1, id), QueryBuilder.set("\"user_Name\"", "Peter"), QueryBuilder.set("data", ByteBuffer.wrap(new byte[]{1, 2, 3})));

        Example other = cassandra.selectOne(Example.class, 1, id).get();
        assertEquals("Peter", other.getUserName());
        assertArrayEquals(new byte[]{1, 2, 3}, other.getData());
        assertEquals(0, other.getNumber());
        assertEquals(false, other.isEnable());
    }

    @Test
    public void test18_DeleteOne() throws Exception {
        cassandra.delete(example);
        assertEquals(5, cassandra.select(Example.class, 1).count());
    }

    @Test
    public void test19_DeleteAll() throws Exception {
        cassandra.delete(Example.class, 1);
        assertEquals(0, cassandra.select(Example.class, 1).count());
    }

    @Test
    public void test20_truncate() throws Exception {
        cassandra.execute("truncate " + entityInfo.table());
    }

    @Test
    public void test21_Insert_Async() throws Exception {
        cassandra.insertAsync(example).get();

        assertThat(cassandra.selectAsync(Example.class, 1).get(), contains(example));

        assertEquals(1, entityPool.entityDataMap.size());
    }

    @Test
    public void test22_Selects_Async() throws Exception {
        cassandra.insertAsync(example1).get();

        assertThat(cassandra.selectAsync(Example.class, 1).get(), containsInAnyOrder(example, example1));
        assertThat(cassandra.selectAsync(Example.class, 1, userId).get(), contains(example));

        assertEquals(example, cassandra.selectOneAsync(Example.class, 1, example.getUserId()).get().get());
        assertEquals(example1, cassandra.selectOneAsync(Example.class, 1, example1.getUserId()).get().get());

        assertFalse(cassandra.selectOneAsync(Example.class, 1, UUID.randomUUID()).get().isPresent());

        Example newExample = new Example(2, UUID.randomUUID());
        assertEquals(newExample, cassandra.selectOneAsync(newExample).get());
    }

    @Test(expected = IllegalArgumentException.class)
    public void test23_SelectTooManyKeys_Async() throws Exception {
        cassandra.selectAsync(Example.class, 1, UUID.randomUUID(), 3).get();
    }

    @Test
    public void test24_SelectNulls_Async() throws Exception {
        UUID id = UUID.randomUUID();
        cassandra.executeAsync(QueryBuilder.insertInto(entityInfo.table()).values(new String[]{"user_rank", "user_id"}, new Object[]{1, id})).get();
        Example other = cassandra.selectOneAsync(Example.class, 1, id).get().get();
        assertEquals(0, other.getNumber());
        assertEquals(false, other.isEnable());
    }

    @Test
    public void test25_InsertSomeFields_Async() throws Exception {
        UUID id = UUID.randomUUID();
        cassandra.insertAsync(new Example(1, id, "Mike", new byte[]{3, 2, 1}, 5, true, RetentionPolicy.CLASS), "\"user_Name\"", "data").get();

        Example other = cassandra.selectOneAsync(Example.class, 1, id).get().get();
        assertEquals("Mike", other.getUserName());
        assertArrayEquals(new byte[]{3, 2, 1}, other.getData());
        assertEquals(0, other.getNumber());
        assertEquals(false, other.isEnable());
    }

    @Test
    public void test26_Update_Async() throws Exception {
        UUID id = UUID.randomUUID();
        final Example updatedEntity = new Example(1, id, "Jon", new byte[]{1, 2, 3}, 5, true, RetentionPolicy.CLASS);
        cassandra.updateAsync(updatedEntity).get();

        assertEquals(updatedEntity, cassandra.selectOneAsync(Example.class, 1, id).get().get());
    }

    @Test
    public void test27_UpdateSomeFields_Async() throws Exception {
        UUID id = UUID.randomUUID();
        cassandra.updateAsync(new Example(1, id), QueryBuilder.set("\"user_Name\"", "Peter"), QueryBuilder.set("data", ByteBuffer.wrap(new byte[]{1, 2, 3}))).get();

        Example other = cassandra.selectOneAsync(Example.class, 1, id).get().get();
        assertEquals("Peter", other.getUserName());
        assertArrayEquals(new byte[]{1, 2, 3}, other.getData());
        assertEquals(0, other.getNumber());
        assertEquals(false, other.isEnable());
    }

    @Test
    public void test28_DeleteOne_Async() throws Exception {
        cassandra.deleteAsync(example).get();
        assertEquals(5, cassandra.selectAsync(Example.class, 1).get().size());
    }

    @Test
    public void test29_DeleteAll_Async() throws Exception {
        cassandra.deleteAsync(Example.class, 1).get();
        assertEquals(0, cassandra.selectAsync(Example.class, 1).get().size());
    }

    private <T> Iterable<T> iterable(Stream<T> stream) {
        return stream::iterator;
    }

    @Table("\"test_streaming_Cassandra\"")
    private static class Example {
        private byte[] data;
        @Key(2)
        private UUID user_id;
        @Key(1)
        private int user_rank;
        @Column("\"user_Name\"")
        protected String user_name;
        public int number;
        public boolean enable;

        public RetentionPolicy retention_policy;

        public Example() {
        }

        private Example(int user_rank, UUID user_id) {
            this.user_rank = user_rank;
            this.user_id = user_id;
        }

        public Example(int user_rank, UUID user_id, String user_name) {
            this.user_rank = user_rank;
            this.user_id = user_id;
            this.user_name = user_name;
        }

        public Example(int user_rank, UUID user_id, String user_name, byte[] data, int number, boolean enable, RetentionPolicy retention_policy) {
            this.user_rank = user_rank;
            this.user_id = user_id;
            this.user_name = user_name;
            this.data = data;
            this.number = number;
            this.enable = enable;
            this.retention_policy = retention_policy;
        }

        public int getUserRank() {
            return user_rank;
        }

        public void setUserRank(int user_rank) {
            this.user_rank = user_rank;
        }

        public UUID getUserId() {
            return user_id;
        }

        public void setUserId(UUID user_token) {
            this.user_id = user_token;
        }

        public String getUserName() {
            return user_name;
        }

        public void setUserName(String user_name) {
            this.user_name = user_name;
        }

        public byte[] getData() {
            return data;
        }

        public void setData(byte[] data) {
            this.data = data;
        }

        public int getNumber() {
            return number;
        }

        public void setNumber(int number) {
            this.number = number;
        }

        public boolean isEnable() {
            return enable;
        }

        public void setEnable(boolean enable) {
            this.enable = enable;
        }

        public RetentionPolicy getRetentionPolicy() {
            return retention_policy;
        }

        public void setRetentionPolicy(RetentionPolicy retentionPolicy) {
            this.retention_policy = retentionPolicy;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Example example = (Example) o;

            if (enable != example.enable) return false;
            if (number != example.number) return false;
            if (user_rank != example.user_rank) return false;
            if (!Arrays.equals(data, example.data)) return false;
            if (retention_policy != example.retention_policy) return false;
            if (!user_id.equals(example.user_id)) return false;
            if (!user_name.equals(example.user_name)) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = data != null ? Arrays.hashCode(data) : 0;
            result = 31 * result + user_id.hashCode();
            result = 31 * result + user_rank;
            result = 31 * result + user_name.hashCode();
            result = 31 * result + number;
            result = 31 * result + (enable ? 1 : 0);
            result = 31 * result + (retention_policy != null ? retention_policy.hashCode() : 0);
            return result;
        }

        @Override
        public String toString() {
            return "Entity{" +
                    "data=" + Arrays.toString(data) +
                    ", user_id=" + user_id +
                    ", user_rank=" + user_rank +
                    ", user_name='" + user_name + '\'' +
                    ", number=" + number +
                    ", flag=" + enable +
                    ", retention_policy=" + retention_policy +
                    '}';
        }
    }

}