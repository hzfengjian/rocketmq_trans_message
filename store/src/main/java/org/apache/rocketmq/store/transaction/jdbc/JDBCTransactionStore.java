/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.store.transaction.jdbc;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.util.JdbcUtils;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.transaction.TransactionRecord;
import org.apache.rocketmq.store.transaction.TransactionStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class JDBCTransactionStore implements TransactionStore {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.TRANSACTION_LOGGER_NAME);
    private final MessageStoreConfig messageStoreConfig;
    private AtomicLong totalRecordsValue = new AtomicLong(0);

    private DruidDataSource druidDataSource;

    private int tableSuffix = -1;

    public JDBCTransactionStore(MessageStoreConfig messageStoreConfig) {
        this.messageStoreConfig = messageStoreConfig;
    }

    @Override
    public boolean load() {
        try {
            druidDataSource = new DruidDataSource();
            druidDataSource.setDriverClassName(messageStoreConfig.getJdbcDriverClass());
            druidDataSource.setUrl(messageStoreConfig.getJdbcURL());
            druidDataSource.setUsername(messageStoreConfig.getJdbcUser());
            druidDataSource.setPassword(messageStoreConfig.getJdbcPassword());
            druidDataSource.setInitialSize(5);
            druidDataSource.setMaxActive(10);
            druidDataSource.setMinIdle(5);
            druidDataSource.setMaxWait(2000);
            druidDataSource.setMinEvictableIdleTimeMillis(300000);
            druidDataSource.setUseUnfairLock(true);
            druidDataSource.setConnectionErrorRetryAttempts(3);
            druidDataSource.setValidationQuery("SELECT 'x'");
            druidDataSource.setTestOnReturn(false);
            druidDataSource.setTestOnBorrow(false);
            druidDataSource.init();
            return true;
        } catch (Exception e) {
            log.info("druidDataSource load Exeption", e);
            return false;
        }
    }

    @Override
    public boolean createTableIfNotExists() {
        try {
            return this.computeTotalRecords() || this.createDB();
        } catch (Exception e) {
            log.info("druidDataSource open Exeption", e);
        }
        return false;
    }


    private boolean computeTotalRecords() {
        Statement statement = null;
        ResultSet resultSet = null;
        Connection connection = null;
        try {
            connection = druidDataSource.getConnection();
            statement = connection.createStatement();
            resultSet = statement.executeQuery(String.format("select count(offset) as total from t_transaction_%d", tableSuffix));
            if (!resultSet.next()) {
                log.warn("computeTotalRecords ResultSet is empty");
                return false;
            }

            this.totalRecordsValue.set(resultSet.getLong(1));
        } catch (Exception e) {
            log.warn("computeTotalRecords Exception", e);
            return false;
        } finally {
            JdbcUtils.close(resultSet);
            JdbcUtils.close(statement);
            JdbcUtils.close(connection);
        }

        return true;
    }

    private boolean createDB() {
        Statement statement = null;
        Connection connection = null;
        try {
            connection = druidDataSource.getConnection();
            connection.setAutoCommit(false);
            statement = connection.createStatement();
            String sql = this.createTableSql();
            log.info("createDB SQL:\n {}", sql);
            statement.execute(sql);
            connection.commit();
            return true;
        } catch (Exception e) {
            log.warn("createDB Exception", e);
            return false;
        } finally {
            JdbcUtils.close(statement);
            JdbcUtils.close(connection);
        }
    }

    private String createTableSql() {
        URL resource = JDBCTransactionStore.class.getClassLoader().getResource("transaction.sql");
        String fileContent = MixAll.file2String(resource);
        return String.format(fileContent, tableSuffix);
    }

    @Override
    public void close() {
        druidDataSource.close();
    }

    @Override
    public boolean parpare(List<TransactionRecord> trs) {
        if (trs == null || trs.size() == 0) {
            return true;
        }
        Connection connection = null;
        PreparedStatement statement = null;
        try {
            connection = druidDataSource.getConnection();
            connection.setAutoCommit(false);
            statement = connection.prepareStatement(String.format("insert into t_transaction_%d (offset,producerGroup,timestamp,size) values (?, ?, ?, ?)", tableSuffix));
            for (TransactionRecord tr : trs) {
                statement.setLong(1, tr.getOffset());
                statement.setString(2, tr.getProducerGroup());
                statement.setLong(3, tr.getTimestamp());
                statement.setInt(4, tr.getSize());
                statement.addBatch();
            }
            int[] executeBatch = statement.executeBatch();
            connection.commit();
            this.totalRecordsValue.addAndGet(updatedRows(executeBatch));
            return true;
        } catch (Exception e) {
            log.warn("createDB Exception", e);
            return false;
        } finally {
            JdbcUtils.close(statement);
            JdbcUtils.close(connection);
        }
    }

    private long updatedRows(int[] rows) {
        long res = 0;
        for (int i : rows) {
            res += i;
        }
        return res;
    }

    @Override
    public boolean confirm(List<Long> pks) {
        if (pks == null || pks.size() == 0) {
            return true;
        }
        PreparedStatement statement = null;
        Connection connection = null;
        try {
            connection = druidDataSource.getConnection();
            connection.setAutoCommit(false);
            statement = connection.prepareStatement(String.format("DELETE FROM t_transaction_%d WHERE offset = ?", tableSuffix));
            for (long pk : pks) {
                statement.setLong(1, pk);
                statement.addBatch();
            }
            int[] executeBatch = statement.executeBatch();
            connection.commit();
            this.totalRecordsValue.addAndGet(-updatedRows(executeBatch));
            return true;
        } catch (Exception e) {
            log.warn("createDB Exception", e);
        } finally {
            JdbcUtils.close(statement);
            JdbcUtils.close(connection);
        }
        return false;
    }

    @Override
    public List<TransactionRecord> traverse(long pk, int nums) {
        List<TransactionRecord> list = new ArrayList<TransactionRecord>(nums);
        Connection connection = null;
        PreparedStatement ps = null;
        try {
            connection = druidDataSource.getConnection();
            ps = connection.prepareStatement(String.format("select offset,producerGroup,timestamp,size from t_transaction_%d where offset>? order by offset limit ?", tableSuffix));
            ps.setLong(1, pk);
            ps.setInt(2, nums);
            ResultSet resultSet = ps.executeQuery();
            while (resultSet.next()) {
                TransactionRecord tr = new TransactionRecord(
                        resultSet.getLong("offset"),
                        resultSet.getLong("timestamp"),
                        resultSet.getInt("size"),
                        resultSet.getString("producerGroup")
                );
                list.add(tr);
            }
            return list;
        } catch (SQLException e) {
            log.warn("traverse Exception", e);
        } finally {
            JdbcUtils.close(ps);
            JdbcUtils.close(connection);
        }
        return list;
    }

    @Override
    public long totalRecords() {
        return this.totalRecordsValue.get();
    }

    @Override
    public long maxPK() {
        Connection connection = null;
        PreparedStatement ps = null;
        try {
            connection = druidDataSource.getConnection();
            ps = connection.prepareStatement(String.format("select max(offset) as maxOffset from t_transaction_%d", tableSuffix));
            ResultSet resultSet = ps.executeQuery();
            if (resultSet.next()) {
                Long maxOffset = resultSet.getLong("maxOffset");
                return maxOffset != null ? maxOffset : -1L;
            }
        } catch (SQLException e) {
            log.warn("maxPK Exception", e);
        } finally {
            JdbcUtils.close(ps);
            JdbcUtils.close(connection);
        }
        return -1L;
    }

    @Override
    public long minPK() {
        Connection connection = null;
        PreparedStatement ps = null;
        try {
            connection = druidDataSource.getConnection();
            ps = connection.prepareStatement(String.format("select min(offset) as minOffset from t_transaction_%d", tableSuffix));
            ResultSet resultSet = ps.executeQuery();
            if (resultSet.next()) {
                Long minOffset = resultSet.getLong("minOffset");
                return minOffset != null ? minOffset : -1L;
            }
        } catch (SQLException e) {
            log.warn("maxPK Exception", e);
        } finally {
            JdbcUtils.close(ps);
            JdbcUtils.close(connection);
        }
        return -1L;
    }

    public void setTableSuffix(int tableSuffix) {
        this.tableSuffix = tableSuffix;
    }
}
