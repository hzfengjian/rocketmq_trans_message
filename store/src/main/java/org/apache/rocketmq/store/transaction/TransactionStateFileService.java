package org.apache.rocketmq.store.transaction;

import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.store.*;
import org.apache.rocketmq.store.config.BrokerRole;
import org.apache.rocketmq.store.config.StorePathConfigHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author fengjian
 * @version V1.0
 * @title: rocketmq-all
 * @Package org.apache.rocketmq.store.transaction
 * @Description:
 * @date 2017/10/25 下午8:37
 */
public final class TransactionStateFileService implements TransactionStateService {
    // 存储单元大小
    public static final int TSS_STORE_UNIT_SIZE = 24;
    // 用来恢复事务状态表的redolog
    public static final String TRANSACTION_REDOLOG_TOPIC = "TRANSACTION_REDOLOG_TOPIC_XXXX";
    public static final int TRANSACTION_REDOLOG_TOPIC_QUEUEID = 0;
    public final static long PREPARED_MESSAGE_TAGS_CODE = -1;
    private static final Logger log = LoggerFactory.getLogger(TransactionStateFileService.class);
    // 更改事务状态，具体更改位置
    private final static int TS_STATE_POS = 20;
    private static final Logger tranlog = LoggerFactory.getLogger(TransactionStateFileService.class);
    // 存储顶层对象
    private final DefaultMessageStore defaultMessageStore;
    // 重复利用内存Buffer
    private final ByteBuffer byteBufferAppend = ByteBuffer.allocate(TSS_STORE_UNIT_SIZE);
    // 事务状态的Redolog，当进程意外宕掉，可通过redolog恢复所有事务的状态
    // Redolog的实现利用了消费队列，主要为了恢复方便
    private final ConsumeQueue tranRedoLog;
    // State Table Offset，重启时，必须纠正
    private final AtomicLong tranStateTableOffset = new AtomicLong(0);
    // 定时回查线程
    private final Timer timer = new Timer("CheckFileTransactionMessageTimer", true);
    // 存储事务状态的表格
    private MappedFileQueue tranStateTable;


    public TransactionStateFileService(final DefaultMessageStore defaultMessageStore) {
        this.defaultMessageStore = defaultMessageStore;

        this.tranStateTable =
                new MappedFileQueue(StorePathConfigHelper.getTranStateTableStorePath(defaultMessageStore
                        .getMessageStoreConfig().getStorePathRootDir()), defaultMessageStore
                        .getMessageStoreConfig().getTranStateTableMapedFileSize(), null);

        this.tranRedoLog =
                new ConsumeQueue(//
                        TRANSACTION_REDOLOG_TOPIC,//
                        TRANSACTION_REDOLOG_TOPIC_QUEUEID,//
                        StorePathConfigHelper.getTranRedoLogStorePath(defaultMessageStore.getMessageStoreConfig()
                                .getStorePathRootDir()),//
                        defaultMessageStore.getMessageStoreConfig().getTranRedoLogMapedFileSize(),//
                        defaultMessageStore);
    }


    public boolean load() {
        boolean result = this.tranRedoLog.load();
        result = result && this.tranStateTable.load();
        return result;
    }


    public void start() {
        this.initTimerTask();
    }


    /**
     * 初始化定时任务
     */
    private void initTimerTask() {
        // 每个文件初始化定时任务
        final List<MappedFile> mapedFiles = this.tranStateTable.getMappedFiles();
        for (MappedFile mf : mapedFiles) {
            this.addTimerTask(mf);
        }
    }

    /**
     * 每个文件初始化定时任务
     *
     * @param mf 文件
     */
    private void addTimerTask(final MappedFile mf) {
        this.timer.scheduleAtFixedRate(new TimerTask() {
            private final MappedFile mapedFile = mf;
            private final TransactionCheckExecuter transactionCheckExecuter = TransactionStateFileService.this.defaultMessageStore.getTransactionCheckExecuter();
            private final long checkTransactionMessageAtleastInterval = TransactionStateFileService.this.defaultMessageStore.getMessageStoreConfig()
                    .getCheckTransactionMessageAtleastInterval();
            private final boolean slave = TransactionStateFileService.this.defaultMessageStore.getMessageStoreConfig().getBrokerRole() == BrokerRole.SLAVE;

            @Override
            public void run() {
                // Slave不需要回查事务状态
                if (slave) {
                    return;
                }
                // Check功能是否开启
                if (!TransactionStateFileService.this.defaultMessageStore.getMessageStoreConfig()
                        .isCheckTransactionMessageEnable()) {
                    return;
                }

                try {
                    SelectMappedBufferResult selectMapedBufferResult = mapedFile.selectMappedBuffer(0);
                    if (selectMapedBufferResult != null) {
                        long preparedMessageCountInThisMapedFile = 0; // 回查的【half消息】数量
                        int i = 0;
                        try {
                            // 循环每条【事务消息】状态，对【half消息】进行回查
                            for (; i < selectMapedBufferResult.getSize(); i += TSS_STORE_UNIT_SIZE) {
                                selectMapedBufferResult.getByteBuffer().position(i);

                                // Commit Log Offset
                                long clOffset = selectMapedBufferResult.getByteBuffer().getLong();
                                // Message Size
                                int msgSize = selectMapedBufferResult.getByteBuffer().getInt();
                                // Timestamp
                                int timestamp = selectMapedBufferResult.getByteBuffer().getInt();
                                // Producer Group Hashcode
                                int groupHashCode = selectMapedBufferResult.getByteBuffer().getInt();
                                // Transaction State
                                int tranType = selectMapedBufferResult.getByteBuffer().getInt();

                                // 已经提交或者回滚的消息跳过
                                if (tranType != MessageSysFlag.TRANSACTION_PREPARED_TYPE) {
                                    continue;
                                }

                                // 遇到时间不符合最小轮询间隔，终止
                                long timestampLong = timestamp * 1000;
                                long diff = System.currentTimeMillis() - timestampLong;
                                if (diff < checkTransactionMessageAtleastInterval) {
                                    break;
                                }

                                preparedMessageCountInThisMapedFile++;

                                // 回查Producer
                                try {
                                    this.transactionCheckExecuter.gotoCheck(groupHashCode, getTranStateOffset(i), clOffset, msgSize);
                                } catch (Exception e) {
                                    tranlog.warn("gotoCheck Exception", e);
                                }
                            }

                            // 无回查的【half消息】数量，且遍历完，则终止定时任务
                            if (0 == preparedMessageCountInThisMapedFile //
                                    && i == mapedFile.getFileSize()) {
                                tranlog.info("remove the transaction timer task, because no prepared message in this mapedfile[{}]", mapedFile.getFileName());
                                this.cancel();
                            }
                        } finally {
                            selectMapedBufferResult.release();
                        }

                        tranlog.info("the transaction timer task execute over in this period, {} Prepared Message: {} Check Progress: {}/{}", mapedFile.getFileName(),//
                                preparedMessageCountInThisMapedFile, i / TSS_STORE_UNIT_SIZE, mapedFile.getFileSize() / TSS_STORE_UNIT_SIZE);
                    } else if (mapedFile.isFull()) {
                        tranlog.info("the mapedfile[{}] maybe deleted, cancel check transaction timer task", mapedFile.getFileName());
                        this.cancel();
                        return;
                    }
                } catch (Exception e) {
                    log.error("check transaction timer task Exception", e);
                }
            }


            private long getTranStateOffset(final long currentIndex) {
                long offset = (this.mapedFile.getFileFromOffset() + currentIndex) / TransactionStateFileService.TSS_STORE_UNIT_SIZE;
                return offset;
            }
        }, 1000 * 1800, this.defaultMessageStore.getMessageStoreConfig().getCheckTransactionMessageTimerInterval());
    }


    public void shutdown() {
        this.timer.cancel();
    }


    public int deleteExpiredStateFile(long offset) {
        int cnt = this.tranStateTable.deleteExpiredFileByOffset(offset, TSS_STORE_UNIT_SIZE);
        return cnt;
    }

    /**
     * 初始化 TranRedoLog
     *
     * @param lastExitOK 是否正常退出
     */
    public void recoverStateTable(final boolean lastExitOK) {
        if (lastExitOK) {
            this.recoverStateTableNormal();
        } else {
            // 第一步，删除State Table
            this.tranStateTable.destroy();
            // 第二步，通过RedoLog全量恢复StateTable
            this.recreateStateTable();
        }
    }

    /**
     * 扫描 TranRedoLog 重建 StateTable
     */
    private void recreateStateTable() {
        this.tranStateTable = new MappedFileQueue(StorePathConfigHelper.getTranStateTableStorePath(defaultMessageStore
                .getMessageStoreConfig().getStorePathRootDir()), defaultMessageStore
                .getMessageStoreConfig().getTranStateTableMapedFileSize(), null);

        final TreeSet<Long> preparedItemSet = new TreeSet<Long>();

        // 第一步，从头扫描RedoLog
        final long minOffset = this.tranRedoLog.getMinOffsetInQueue();
        long processOffset = minOffset;
        while (true) {
            SelectMappedBufferResult bufferConsumeQueue = this.tranRedoLog.getIndexBuffer(processOffset);
            if (bufferConsumeQueue != null) {
                try {
                    long i = 0;
                    for (; i < bufferConsumeQueue.getSize(); i += ConsumeQueue.CQ_STORE_UNIT_SIZE) {
                        long offsetMsg = bufferConsumeQueue.getByteBuffer().getLong();
                        int sizeMsg = bufferConsumeQueue.getByteBuffer().getInt();
                        long tagsCode = bufferConsumeQueue.getByteBuffer().getLong();

                        if (TransactionStateFileService.PREPARED_MESSAGE_TAGS_CODE == tagsCode) { // Prepared
                            preparedItemSet.add(offsetMsg);
                        } else { // Commit/Rollback
                            preparedItemSet.remove(tagsCode);
                        }
                    }

                    processOffset += i;
                } finally { // 必须释放资源
                    bufferConsumeQueue.release();
                }
            } else {
                break;
            }
        }
        log.info("scan transaction redolog over, End offset: {},  Prepared Transaction Count: {}", processOffset, preparedItemSet.size());

        // 第二步，重建StateTable
        Iterator<Long> it = preparedItemSet.iterator();
        while (it.hasNext()) {
            Long offset = it.next();
            MessageExt msgExt = this.defaultMessageStore.lookMessageByOffset(offset);
            if (msgExt != null) {
                this.appendPreparedTransaction(msgExt.getCommitLogOffset(), msgExt.getStoreSize(),
                        (int) (msgExt.getStoreTimestamp() / 1000),
                        msgExt.getProperty(MessageConst.PROPERTY_PRODUCER_GROUP).hashCode());
                this.tranStateTableOffset.incrementAndGet();
            }
        }
    }

    private boolean appendPreparedTransaction(final long clOffset, final int size, final int timestamp, final int groupHashCode) {
        MappedFile mappedFile = this.tranStateTable.getLastMappedFile(0);
        if (null == mappedFile) {
            log.error("appendPreparedTransaction: create mapedfile error.");
            return false;
        }

        // 首次创建，加入定时任务中
        if (0 == mappedFile.getWrotePosition()) {
            this.addTimerTask(mappedFile);
        }

        this.byteBufferAppend.position(0);
        this.byteBufferAppend.limit(TSS_STORE_UNIT_SIZE);

        // Commit Log Offset
        this.byteBufferAppend.putLong(clOffset);
        // Message Size
        this.byteBufferAppend.putInt(size);
        // Timestamp
        this.byteBufferAppend.putInt(timestamp);
        // Producer Group Hashcode
        this.byteBufferAppend.putInt(groupHashCode);
        // Transaction State
        this.byteBufferAppend.putInt(MessageSysFlag.TRANSACTION_PREPARED_TYPE);

        return mappedFile.appendMessage(this.byteBufferAppend.array());
    }

    /**
     * 新增事务状态
     *
     * @return 是否成功
     */
    @Override
    public boolean appendPreparedTransaction(DispatchRequest request) {
        final long clOffset = request.getCommitLogOffset();
        final int size = request.getMsgSize();
        final int timestamp = (int) (request.getStoreTimestamp() / 1000);
        final int groupHashCode = request.getProducerGroup().hashCode();
        return appendPreparedTransaction(clOffset, size, timestamp, groupHashCode);
    }

    /**
     * 加载（解析）TranStateTable 的 MappedFile
     * 1. 清理多余 MappedFile，设置最后一个 MappedFile的写入位置(position
     * 2. 设置 TanStateTable 最大物理位置（可写入位置）
     */
    private void recoverStateTableNormal() {
        final List<MappedFile> mapedFiles = this.tranStateTable.getMappedFiles();
        if (!mapedFiles.isEmpty()) {
            // 从倒数第三个文件开始恢复
            int index = mapedFiles.size() - 3;
            if (index < 0) {
                index = 0;
            }

            int mapedFileSizeLogics = this.tranStateTable.getMappedFileSize();
            MappedFile mapedFile = mapedFiles.get(index);
            ByteBuffer byteBuffer = mapedFile.sliceByteBuffer();
            long processOffset = mapedFile.getFileFromOffset();
            long mapedFileOffset = 0;
            while (true) {
                for (int i = 0; i < mapedFileSizeLogics; i += TSS_STORE_UNIT_SIZE) {

                    final long clOffset_read = byteBuffer.getLong();
                    final int size_read = byteBuffer.getInt();
                    final int timestamp_read = byteBuffer.getInt();
                    final int groupHashCode_read = byteBuffer.getInt();
                    final int state_read = byteBuffer.getInt();

                    boolean stateOK = false;
                    switch (state_read) {
                        case MessageSysFlag.TRANSACTION_PREPARED_TYPE:
                        case MessageSysFlag.TRANSACTION_COMMIT_TYPE:
                        case MessageSysFlag.TRANSACTION_ROLLBACK_TYPE:
                            stateOK = true;
                            break;
                        default:
                            break;
                    }

                    // 说明当前存储单元有效
                    if (clOffset_read >= 0 && size_read > 0 && stateOK) {
                        mapedFileOffset = i + TSS_STORE_UNIT_SIZE;
                    } else {
                        log.info("recover current transaction state table file over,  " + mapedFile.getFileName() + " "
                                + clOffset_read + " " + size_read + " " + timestamp_read);
                        break;
                    }
                }

                // 走到文件末尾，切换至下一个文件
                if (mapedFileOffset == mapedFileSizeLogics) {
                    index++;
                    if (index >= mapedFiles.size()) { // 循环while结束
                        log.info("recover last transaction state table file over, last maped file " + mapedFile.getFileName());
                        break;
                    } else { // 切换下一个文件
                        mapedFile = mapedFiles.get(index);
                        byteBuffer = mapedFile.sliceByteBuffer();
                        processOffset = mapedFile.getFileFromOffset();
                        mapedFileOffset = 0;
                        log.info("recover next transaction state table file, " + mapedFile.getFileName());
                    }
                } else {
                    log.info("recover current transaction state table queue over " + mapedFile.getFileName() + " " + (processOffset + mapedFileOffset));
                    break;
                }
            }

            // 清理多余 MappedFile，设置最后一个 MappedFile的写入位置(position
            processOffset += mapedFileOffset;
            this.tranStateTable.truncateDirtyFiles(processOffset);

            // 设置 TanStateTable 最大物理位置（可写入位置）
            this.tranStateTableOffset.set(this.tranStateTable.getMaxOffset() / TSS_STORE_UNIT_SIZE);
            log.info("recover normal over, transaction state table max offset: {}", this.tranStateTableOffset.get());
        }
    }


    /**
     * 更新事务状态
     *
     * @return 是否成功
     */
    @Override
    public boolean updateTransactionState(DispatchRequest request) {
        final long tsOffset = request.getTranStateTableOffset();
        final long clOffset = request.getPreparedTransactionOffset();
        final int groupHashCode = request.getProducerGroup().hashCode();
        final int state = MessageSysFlag.getTransactionValue(request.getSysFlag());
        SelectMappedBufferResult selectMapedBufferResult = this.findTransactionBuffer(tsOffset);
        if (selectMapedBufferResult != null) {
            try {
                final long clOffset_read = selectMapedBufferResult.getByteBuffer().getLong();
                final int size_read = selectMapedBufferResult.getByteBuffer().getInt();
                final int timestamp_read = selectMapedBufferResult.getByteBuffer().getInt();
                final int groupHashCode_read = selectMapedBufferResult.getByteBuffer().getInt();
                final int state_read = selectMapedBufferResult.getByteBuffer().getInt();

                // 校验数据正确性
                if (clOffset != clOffset_read) {
                    log.error("updateTransactionState error clOffset: {} clOffset_read: {}", clOffset,
                            clOffset_read);
                    return false;
                }

                // 校验数据正确性
                if (groupHashCode != groupHashCode_read) {
                    log.error("updateTransactionState error groupHashCode: {} groupHashCode_read: {}",
                            groupHashCode, groupHashCode_read);
                    return false;
                }

                // 判断是否已经更新过
                if (MessageSysFlag.TRANSACTION_PREPARED_TYPE != state_read) {
                    log.warn("updateTransactionState error, the transaction is updated before.");
                    return true;
                }

                // 更新事务状态
                selectMapedBufferResult.getByteBuffer().putInt(TS_STATE_POS, state);
            } catch (Exception e) {
                log.error("updateTransactionState exception", e);
            } finally {
                selectMapedBufferResult.release();
            }
        }

        return false;
    }


    private SelectMappedBufferResult findTransactionBuffer(final long tsOffset) {
        final int mapedFileSize =
                this.defaultMessageStore.getMessageStoreConfig().getTranStateTableMapedFileSize();
        final long offset = tsOffset * TSS_STORE_UNIT_SIZE;
        MappedFile mapedFile = this.tranStateTable.findMappedFileByOffset(offset);
        if (mapedFile != null) {
            SelectMappedBufferResult result = mapedFile.selectMappedBuffer((int) (offset % mapedFileSize));
            return result;
        }

        return null;
    }


    public AtomicLong getTranStateTableOffset() {
        return tranStateTableOffset;
    }

    @Override
    public long getMaxTransOffset() {
        return 0;
    }

    @Override
    public long getMinTransOffset() {
        return 0;
    }


    public ConsumeQueue getTranRedoLog() {
        return tranRedoLog;
    }
}
