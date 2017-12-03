package org.apache.rocketmq.store.transaction;

import org.apache.rocketmq.store.ConsumeQueue;
import org.apache.rocketmq.store.DispatchRequest;

import java.util.concurrent.atomic.AtomicLong;

/**
 * @author fengjian
 * @version V1.0
 * @title: rocketmq-all
 * @Package org.apache.rocketmq.store.transaction
 * @Description: 事务接口
 * @date 2017/11/3 下午4:17
 */
public interface TransactionStateService {

    boolean load();

    void start();

    void shutdown();

    int deleteExpiredStateFile(long offset);

    void recoverStateTable(final boolean lastExitOK);

    boolean appendPreparedTransaction(DispatchRequest request);

    boolean updateTransactionState(DispatchRequest request);

    ConsumeQueue getTranRedoLog();

    AtomicLong getTranStateTableOffset();

    public long getMaxTransOffset();

    public long getMinTransOffset();
}
