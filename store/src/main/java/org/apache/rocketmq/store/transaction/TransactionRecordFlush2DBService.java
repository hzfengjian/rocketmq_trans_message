package org.apache.rocketmq.store.transaction;

import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.common.utils.ThreadUtils;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.DispatchRequest;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.transaction.jdbc.JDBCTransactionStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author fengjian
 * @version V1.0
 * @title: rocketmq-all
 * @Package org.apache.rocketmq.store.transaction
 * @Description:
 * @date 2017/11/7 上午11:25
 */
public class TransactionRecordFlush2DBService extends ServiceThread {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.TRANSACTION_LOGGER_NAME);

    private final TransactionOffsetConifgService transactionOffsetConifgService;

    private final TransactionStore transactionStore;

    public TransactionRecordFlush2DBService(DefaultMessageStore defaultMessageStore) {
        MessageStoreConfig messageStoreConfig = defaultMessageStore.getMessageStoreConfig();
        this.transactionOffsetConifgService = new TransactionOffsetConifgService(defaultMessageStore);
        TransactionTableDefConfigService transactionTableDefConfigService = new TransactionTableDefConfigService(defaultMessageStore);
        transactionTableDefConfigService.load();
        int retry = 0;
        boolean isCheckTable = false;
        this.transactionStore = new JDBCTransactionStore(messageStoreConfig);
        this.transactionStore.load();
        do {
            int tableSuffix = transactionTableDefConfigService.getTableSuffix(messageStoreConfig);
            this.transactionStore.setTableSuffix(tableSuffix);
            log.info("loadTableStoreConfig tableSuffix={}", tableSuffix);
        } while (!(isCheckTable = transactionStore.createTableIfNotExists()) && ++retry < 5);

        if (!isCheckTable) {
            throw new RuntimeException("check db info error!");
        } else {
            transactionTableDefConfigService.persist();
        }

        for (int i = 0; i < REQUEST_BUFFER_IN_QUEUE; i++) {
            dispatchRequestBufferQueue.add(new DispatchRequestCollections(new AtomicInteger(0), new ArrayList<>()));
        }
    }


    public AtomicLong queryTransactionOffset() {
        return transactionOffsetConifgService.queryOffset();
    }

    @Override
    public void start() {
        super.start();
        transactionOffsetConifgService.start();
    }

    public void load() {
        transactionOffsetConifgService.load();

        long maxOffset = transactionStore.maxPK();
        long minOffset = transactionStore.minPK();
        long transactionOffset = queryTransactionOffset().get();
        //set parpare offset
        if (maxOffset > transactionOffset) {
            queryTransactionOffset().compareAndSet(transactionOffset, maxOffset);
            this.maxTransOffset.set(maxOffset);
        } else {
            this.maxTransOffset.set(transactionOffset);
        }
        //set confirm offset
        if (minOffset < transactionOffset && minOffset > 0) {
            this.minTransOffset.set(minOffset);
        } else {
            this.minTransOffset.set(transactionOffset);
        }

    }

    class DispatchRequestCollections {
        private AtomicInteger latch;
        private List<DispatchRequest> requestlist;

        DispatchRequestCollections(AtomicInteger latch, List<DispatchRequest> requestlist) {
            this.latch = latch;
            this.requestlist = requestlist;
        }
    }

    private static final int REQUEST_BUFFER_IN_QUEUE = 5;

    private final AtomicLong maxTransOffset = new AtomicLong(0);

    private final AtomicLong minTransOffset = new AtomicLong(0);

    private static final int FLOW_CONTROLLER = 20000;

    private static final int CHECK_THREAD_LOOP = 100;

    private volatile int flushCounter = 0;

    private volatile ConcurrentLinkedQueue<DispatchRequestCollections> dispatchRequestBufferQueue = new ConcurrentLinkedQueue<>();

    private volatile Semaphore flowController = new Semaphore(FLOW_CONTROLLER);

    private void putEmptyRequestList() {
        dispatchRequestBufferQueue.add(new DispatchRequestCollections(new AtomicInteger(0), new CopyOnWriteArrayList<>()));
    }

    @Override
    public String getServiceName() {
        return TransactionRecordFlush2DBService.class.getName();
    }

    public void appendPreparedTransaction(DispatchRequest dispatchRequest) {
        try {
            flowController.acquire(1);
            while (true) {
                DispatchRequestCollections requests = dispatchRequestBufferQueue.peek();
                if (requests.latch.getAndIncrement() >= 0) {
                    requests.requestlist.add(dispatchRequest);
                    break;
                }
            }
        } catch (InterruptedException e) {
            log.error("putDispatchRequest interrupted");
        }
    }

    @Override
    public void run() {
        log.info(this.getServiceName() + " service started");

        while (!this.isStopped()) {
            try {
                DispatchRequestCollections requests = dispatchRequestBufferQueue.peek();
                if (requests.requestlist.size() > FLOW_CONTROLLER / REQUEST_BUFFER_IN_QUEUE
                        || flushCounter > CHECK_THREAD_LOOP) {
                    this.doFlushDB(false);
                    flushCounter = 0;
                    continue;
                }
                ++flushCounter;
                ThreadUtils.sleep(10);
            } catch (Exception e) {
                log.warn(this.getServiceName() + " service has exception. ", e);
            }
        }
        log.info(this.getServiceName() + " service end");
    }

    private void doFlushDB(boolean shutdown) {
        DispatchRequestCollections requests = dispatchRequestBufferQueue.poll();
        if (requests == null) {
            return;
        }
        if (!shutdown) {
            putEmptyRequestList();
        }
        boolean addSuccess = false, removeSuccess = false;
        LinkedHashMap<Long, TransactionRecord> prepareTrs = null;
        LinkedHashMap<Long, Void> confirmTrs = null;
        while (true) {
            if (requests.latch.get() != requests.requestlist.size() && requests.latch.get() > 0) {
                continue;
            }
            requests.latch.set(Integer.MIN_VALUE);

            if (requests.requestlist.size() == 0) {
                break;
            }
            try {
                //数据处理
                if (prepareTrs == null && confirmTrs == null) {
                    prepareTrs = new LinkedHashMap<Long, TransactionRecord>();
                    confirmTrs = new LinkedHashMap<Long, Void>();
                    for (DispatchRequest request : requests.requestlist) {
                        final int tranType = MessageSysFlag.getTransactionValue(request.getSysFlag());
                        switch (tranType) {
                            case MessageSysFlag.TRANSACTION_NOT_TYPE:
                                break;
                            case MessageSysFlag.TRANSACTION_PREPARED_TYPE:
                                if (this.maxTransOffset.get() < request.getCommitLogOffset()) {
                                    prepareTrs.put(request.getCommitLogOffset(), new TransactionRecord(request.getCommitLogOffset(),
                                            request.getCheckImmunityTimeOutTimestamp(), request.getMsgSize(), request.getProducerGroup()));
                                    this.maxTransOffset.set(request.getCommitLogOffset());
                                } else {
                                    log.info("[PREPARED] request ignore offset =" + request.getCommitLogOffset());
                                }
                                if (request.getPreparedTransactionOffset() == 0L) {
                                    break;
                                }
                            case MessageSysFlag.TRANSACTION_COMMIT_TYPE:
                            case MessageSysFlag.TRANSACTION_ROLLBACK_TYPE:
                                if (this.minTransOffset.get() < request.getCommitLogOffset()) {
                                    if (prepareTrs.containsKey(request.getPreparedTransactionOffset())) {
                                        prepareTrs.remove(request.getPreparedTransactionOffset());
                                    } else {
                                        confirmTrs.put(request.getPreparedTransactionOffset(), null);
                                    }
                                } else {
                                    log.info("[COMMIT] request ignore offset =" + request.getCommitLogOffset());
                                }
                                break;
                        }
                    }
                    long transactionOffset = requests.requestlist.get(requests.requestlist.size() - 1).getCommitLogOffset();
                    transactionOffsetConifgService.putOffset(transactionOffset);
                }

                long startTime = System.currentTimeMillis();
                addSuccess = addSuccess || transactionStore.parpare(new ArrayList<>(prepareTrs.values()));
                if (addSuccess && (removeSuccess = transactionStore.confirm(new ArrayList<>(confirmTrs.keySet())))) {
                    log.info("pull TransactionRecord consume {}ms ,size={},realParpareSize={},realConfirmSize:{}",
                            (System.currentTimeMillis() - startTime), requests.requestlist.size(), prepareTrs.size(), confirmTrs.size());
                    break;
                }
            } catch (Throwable e) {
                log.error("transactionStore error:", e);
                ThreadUtils.sleep(2000);
            } finally {
                if (addSuccess && removeSuccess) {
                    flowController.release(requests.requestlist.size());
                }
            }
        }
    }


    public void shutdown() {
        super.shutdown();
        while (dispatchRequestBufferQueue.peek() != null) {
            this.doFlushDB(true);
        }
        transactionOffsetConifgService.persist();
    }

    public TransactionStore getTransactionStore() {
        return transactionStore;
    }

    public long getMaxTransOffset() {
        return maxTransOffset.get();
    }

    public long getMinTransOffset() {
        return minTransOffset.get();
    }
}

