package org.apache.rocketmq.store.transaction;

import org.apache.rocketmq.common.ConfigManager;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.config.StorePathConfigHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author fengjian
 * @version V1.0
 * @title: rocketmq-all
 * @Package org.apache.rocketmq.store.transaction
 * @Description:
 * @date 2017/11/6 上午11:15
 */
public class TransactionOffsetConifgService extends ConfigManager {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    private final AtomicLong transactionOffset = new AtomicLong(0L);

    private final DefaultMessageStore defaultMessageStore;

    private final Timer timer = new Timer("TransactionOffsetConifgThread", true);

    public TransactionOffsetConifgService(final DefaultMessageStore defaultMessageStore) {
        this.defaultMessageStore = defaultMessageStore;
    }

    public AtomicLong queryOffset() {
        return transactionOffset;
    }

    public void putOffset(Long offset) {
        transactionOffset.set(offset > transactionOffset.get() ? offset : transactionOffset.get());
    }

    @Override
    public String encode() {
        return encode(false);
    }

    @Override
    public String configFilePath() {
        return StorePathConfigHelper.getTranOffsetPath(this.defaultMessageStore.getMessageStoreConfig()
                .getStorePathRootDir());
    }

    @Override
    public void decode(String jsonString) {
        transactionOffset.set(Long.parseLong(jsonString));
    }

    @Override
    public String encode(boolean prettyFormat) {
        return String.valueOf(transactionOffset);
    }

    public void start() {
        this.timer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                try {
                    TransactionOffsetConifgService.this.persist();
                } catch (Exception e) {
                    log.error("scheduleAtFixedRate flush exception", e);
                }
            }
        }, this.defaultMessageStore.getMessageStoreConfig().getFlushDelayTransOffsetInterval(),
                this.defaultMessageStore.getMessageStoreConfig().getFlushDelayTransOffsetInterval());
    }
}
