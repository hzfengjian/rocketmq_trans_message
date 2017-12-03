package org.apache.rocketmq.store.transaction;

import org.apache.rocketmq.common.ConfigManager;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.config.StorePathConfigHelper;

import java.util.UUID;

/**
 * @author fengjian
 * @version V1.0
 * @title: rocketmq-all
 * @Package org.apache.rocketmq.store.transaction
 * @Description:
 * @date 2017/11/19 上午11:50
 */
public class TransactionTableDefConfigService extends ConfigManager {

    private DefaultMessageStore defaultMessageStore;

    private TableDefine tableDefine;

    public TransactionTableDefConfigService(DefaultMessageStore defaultMessageStore) {
        this.defaultMessageStore = defaultMessageStore;
    }

    public void setTableDefine(TableDefine tableDefine) {
        this.tableDefine = tableDefine;
    }

    @Override
    public String encode() {
        if (tableDefine == null) {
            return "";
        }
        return RemotingSerializable.toJson(tableDefine, false);
    }

    @Override
    public String configFilePath() {
        return StorePathConfigHelper.getTranTablePath(this.defaultMessageStore.getMessageStoreConfig()
                .getStorePathRootDir());
    }

    @Override
    public void decode(String jsonString) {
        if (jsonString != null && jsonString.length() > 0) {
            this.tableDefine = RemotingSerializable.fromJson(jsonString, TableDefine.class);
        } else {
            this.tableDefine = null;
        }
    }

    @Override
    public String encode(boolean prettyFormat) {
        return RemotingSerializable.toJson(tableDefine, prettyFormat);
    }

    public int getTableSuffix(MessageStoreConfig messageStoreConfig) {
        TransactionTableDefConfigService.TableDefine tableDefine = getTableDefine();
        if (tableDefine == null) {
            tableDefine = new TransactionTableDefConfigService.TableDefine();
            tableDefine.setTableSuffix(Math.abs(UUID.randomUUID().toString().replace("-", "").hashCode()));
            tableDefine.setUrl(messageStoreConfig.getJdbcURL());
            this.setTableDefine(tableDefine);
        } else {
            if (tableDefine.getUrl() != null && !tableDefine.getUrl().equals(messageStoreConfig.getJdbcURL())) {
                throw new RuntimeException(String.format("database check url isn't match, old url is %s ,and the new url is %s", tableDefine.getUrl(), messageStoreConfig.getJdbcURL()));
            }
        }
        return tableDefine.getTableSuffix();
    }

    public static class TableDefine {

        private int tableSuffix;

        private String url;

        public int getTableSuffix() {
            return tableSuffix;
        }

        public void setTableSuffix(int tableSuffix) {
            this.tableSuffix = tableSuffix;
        }

        public String getUrl() {
            return url;
        }

        public void setUrl(String url) {
            this.url = url;
        }
    }

    public TableDefine getTableDefine() {
        return tableDefine;
    }
}
