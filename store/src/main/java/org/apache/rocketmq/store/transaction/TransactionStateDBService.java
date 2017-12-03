package org.apache.rocketmq.store.transaction;

import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.store.ConsumeQueue;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.DispatchRequest;
import org.apache.rocketmq.store.config.BrokerRole;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author fengjian
 * @version V1.0
 * @title: rocketmq-all
 * @Package org.apache.rocketmq.store.transaction
 * @Description: DB
 * @date 2017/11/3 下午4:45
 */
public class TransactionStateDBService implements TransactionStateService {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.TRANSACTION_LOGGER_NAME);

    private final TransactionRecordFlush2DBService transactionRecordFlush2DBService;

    // 定时回查线程
    private final Timer timer = new Timer("CheckDBTransactionMessageTimer", true);

    private final DefaultMessageStore defaultMessageStore;

    public TransactionStateDBService(DefaultMessageStore defaultMessageStore) {
        this.defaultMessageStore = defaultMessageStore;
        transactionRecordFlush2DBService = new TransactionRecordFlush2DBService(defaultMessageStore);
    }

    @Override
    public boolean load() {
        transactionRecordFlush2DBService.load();
        transactionRecordFlush2DBService.start();
        return true;
    }

    @Override
    public void start() {
        addTimerTask();
    }

    @Override
    public void shutdown() {
        transactionRecordFlush2DBService.shutdown();
    }

    @Override
    public int deleteExpiredStateFile(long offset) {
        return 0;
    }

    @Override
    public void recoverStateTable(boolean lastExitOK) {
        if (!lastExitOK) {
            long transactionOffset = defaultMessageStore.getTransactionStateService().getTranStateTableOffset().get();
            long processOffset = defaultMessageStore.getCommitLog().getMaxOffset();
            if (processOffset > transactionOffset) {
//                log.info("[TransactionLogInDB] processOffset!= transactionOffset,need recover data");
//                List<MappedFile> mfs = this.defaultMessageStore.getCommitLog().findGreaterThanMappedFileByOffset(transactionOffset);
//                for (MappedFile mappedFile : mfs) {
//                    ByteBuffer byteBuffer = mappedFile.sliceByteBuffer();
//                    log.info("[TransactionLogInDB] recover next physics file, " + mappedFile.getFileName());
//                    while (true) {
//                        DispatchRequest dispatchRequest = this.defaultMessageStore.getCommitLog().checkMessageAndReturnSize(byteBuffer, checkCRCOnRecover);
//                        int size = dispatchRequest.getMsgSize();
//                        if (size > 0) {
//                            if (this.defaultMessageStore.getMessageStoreConfig().isDuplicationEnable()) {
//                                if (dispatchRequest.getCommitLogOffset() < this.defaultMessageStore.getConfirmOffset()) {
//                                    this.defaultMessageStore.doDispatch(dispatchRequest);
//                                }
//                            } else {
//                                this.defaultMessageStore.doDispatch(dispatchRequest);
//                            }
//                        } else if (size == -1) {
//                            log.info("[TransactionLogInDB] recover physics file end, " + mappedFile.getFileName());
//                            break;
//                        } else if (size == 0) {
//                            log.info("[TransactionLogInDB] recover next physics file, " + mappedFile.getFileName());
//                            break;
//                        }
//                    }
                //           log.info("[TransactionLogInDB] recover physics file over, last maped file " + mfs.get(mfs.size() - 1).getFileName());
                //       }
            }
        }
    }

    @Override
    public boolean appendPreparedTransaction(DispatchRequest request) {
        try {
            transactionRecordFlush2DBService.appendPreparedTransaction(request);
            return true;
        } catch (Throwable e) {
            log.error(e.getMessage(), e);
            return false;
        }
    }

    @Override
    public boolean updateTransactionState(DispatchRequest request) {
        try {
            transactionRecordFlush2DBService.appendPreparedTransaction(request);
            return true;
        } catch (Throwable e) {
            log.error(e.getMessage(), e);
            return false;
        }
    }


    @Override
    public ConsumeQueue getTranRedoLog() {
        return null;
    }

    @Override
    public AtomicLong getTranStateTableOffset() {
        return transactionRecordFlush2DBService.queryTransactionOffset();
    }

    private void addTimerTask() {
        this.timer.scheduleAtFixedRate(new TimerTask() {
            private final TransactionCheckExecuter transactionCheckExecuter = TransactionStateDBService.this.defaultMessageStore.getTransactionCheckExecuter();
            private final boolean slave = TransactionStateDBService.this.defaultMessageStore.getMessageStoreConfig().getBrokerRole() == BrokerRole.SLAVE;

            @Override
            public void run() {
                // Slave不需要回查事务状态
                if (slave) {
                    return;
                }
                // Check功能是否开启
                if (!TransactionStateDBService.this.defaultMessageStore.getMessageStoreConfig()
                        .isCheckTransactionMessageEnable()) {
                    return;
                }

                long totalRecords = transactionRecordFlush2DBService.getTransactionStore().totalRecords();
                long pk = 0;
                List<TransactionRecord> records = new ArrayList<TransactionRecord>();
                while (totalRecords > 0 && (records = transactionRecordFlush2DBService.getTransactionStore().traverse(pk, 100)).size() > 0) {
                    for (TransactionRecord record : records) {
                        try {
                            long timestampLong = record.getTimestamp();
                            if (System.currentTimeMillis() - timestampLong <= 0) {
                                continue;
                            }
                            this.transactionCheckExecuter.gotoCheck(record.getProducerGroup().hashCode(), 0L, record.getOffset(), record.getSize());
                            pk = record.getOffset();
                        } catch (Exception e) {
                            log.warn("gotoCheck Exception", e);
                        }
                    }
                    totalRecords -= records.size();
                }
            }
        }, 1000 * 20, this.defaultMessageStore.getMessageStoreConfig().getCheckTransactionMessageTimerInterval());
    }

    public long getMaxTransOffset() {
        return transactionRecordFlush2DBService.getMaxTransOffset();
    }

    public long getMinTransOffset() {
        return transactionRecordFlush2DBService.getMinTransOffset();
    }
}
