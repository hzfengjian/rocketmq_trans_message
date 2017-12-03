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
package org.apache.rocketmq.broker.client;

import io.netty.channel.Channel;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.common.RemotingUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class ProducerManager {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
    private static final long LOCK_TIMEOUT_MILLIS = 3000;
    private static final long CHANNEL_EXPIRED_TIMEOUT = 1000 * 120;
    private final Lock groupChannelLock = new ReentrantLock();
    private final HashMap<String /* group name */, HashMap<Channel, ClientChannelInfo>> groupChannelTable =
            new HashMap<String, HashMap<Channel, ClientChannelInfo>>();

    private final Lock hashcodeChannelLock = new ReentrantLock();
    private final HashMap<Integer /* group hash code */, List<ClientChannelInfo>> hashcodeChannelTable =
            new HashMap<Integer, List<ClientChannelInfo>>();

    private final Random random = new Random(System.currentTimeMillis());

    public ProducerManager() {
    }

    public HashMap<String, HashMap<Channel, ClientChannelInfo>> getGroupChannelTable() {
        HashMap<String /* group name */, HashMap<Channel, ClientChannelInfo>> newGroupChannelTable =
                new HashMap<String, HashMap<Channel, ClientChannelInfo>>();
        try {
            if (this.groupChannelLock.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                try {
                    newGroupChannelTable.putAll(groupChannelTable);
                } finally {
                    groupChannelLock.unlock();
                }
            }
        } catch (InterruptedException e) {
            log.error("", e);
        }
        return newGroupChannelTable;
    }

    public void scanNotActiveChannel() {

        try {
            if (this.hashcodeChannelLock.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                try {
                    Iterator<Entry<Integer, List<ClientChannelInfo>>> it =
                            this.hashcodeChannelTable.entrySet().iterator();

                    while (it.hasNext()) {
                        Entry<Integer, List<ClientChannelInfo>> entry = it.next();

                        final Integer groupHashCode = entry.getKey();
                        final List<ClientChannelInfo> clientChannelInfoList = entry.getValue();

                        Iterator<ClientChannelInfo> itChannelInfo = clientChannelInfoList.iterator();
                        while (itChannelInfo.hasNext()) {
                            ClientChannelInfo clientChannelInfo = itChannelInfo.next();
                            long diff =
                                    System.currentTimeMillis() - clientChannelInfo.getLastUpdateTimestamp();
                            if (diff > CHANNEL_EXPIRED_TIMEOUT) {
                                log.warn(
                                        "SCAN: remove expired channel[{}] from ProducerManager hashcodeChannelTable, producer group hash code: {}",
                                        RemotingHelper.parseChannelRemoteAddr(clientChannelInfo.getChannel()),
                                        groupHashCode);
                                RemotingUtil.closeChannel(clientChannelInfo.getChannel());
                                itChannelInfo.remove();
                            }
                        }
                    }
                } finally {
                    this.hashcodeChannelLock.unlock();
                }
            } else {
                log.warn("ProducerManager scanNotActiveChannel lock timeout");
            }
        } catch (InterruptedException e) {
            log.error("", e);
        }


        try {
            if (this.groupChannelLock.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                try {
                    for (final Map.Entry<String, HashMap<Channel, ClientChannelInfo>> entry : this.groupChannelTable
                            .entrySet()) {
                        final String group = entry.getKey();
                        final HashMap<Channel, ClientChannelInfo> chlMap = entry.getValue();

                        Iterator<Entry<Channel, ClientChannelInfo>> it = chlMap.entrySet().iterator();
                        while (it.hasNext()) {
                            Entry<Channel, ClientChannelInfo> item = it.next();
                            // final Integer id = item.getKey();
                            final ClientChannelInfo info = item.getValue();

                            long diff = System.currentTimeMillis() - info.getLastUpdateTimestamp();
                            if (diff > CHANNEL_EXPIRED_TIMEOUT) {
                                it.remove();
                                log.warn(
                                        "SCAN: remove expired channel[{}] from ProducerManager groupChannelTable, producer group name: {}",
                                        RemotingHelper.parseChannelRemoteAddr(info.getChannel()), group);
                                RemotingUtil.closeChannel(info.getChannel());
                            }
                        }
                    }
                } finally {
                    this.groupChannelLock.unlock();
                }
            } else {
                log.warn("ProducerManager scanNotActiveChannel lock timeout");
            }
        } catch (InterruptedException e) {
            log.error("", e);
        }
    }

    public void doChannelCloseEvent(final String remoteAddr, final Channel channel) {
        if (channel != null) {
            try {
                if (this.hashcodeChannelLock.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                    try {
                        for (final Map.Entry<Integer, List<ClientChannelInfo>> entry : this.hashcodeChannelTable
                                .entrySet()) {
                            final Integer groupHashCode = entry.getKey();
                            final List<ClientChannelInfo> clientChannelInfoList = entry.getValue();
                            boolean result = clientChannelInfoList.remove(new ClientChannelInfo(channel));
                            if (result) {
                                log.info(
                                        "NETTY EVENT: remove channel[{}][{}] from ProducerManager hashcodeChannelTable, producer group hash code: {}",
                                        RemotingHelper.parseChannelRemoteAddr(channel), remoteAddr, groupHashCode);
                            }
                        }
                    } finally {
                        this.hashcodeChannelLock.unlock();
                    }
                } else {
                    log.warn("ProducerManager doChannelCloseEvent lock timeout");
                }
            } catch (InterruptedException e) {
                log.error("", e);
            }

            try {
                if (this.groupChannelLock.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                    try {
                        for (final Map.Entry<String, HashMap<Channel, ClientChannelInfo>> entry : this.groupChannelTable
                                .entrySet()) {
                            final String group = entry.getKey();
                            final HashMap<Channel, ClientChannelInfo> clientChannelInfoTable =
                                    entry.getValue();
                            final ClientChannelInfo clientChannelInfo =
                                    clientChannelInfoTable.remove(channel);
                            if (clientChannelInfo != null) {
                                log.info(
                                        "NETTY EVENT: remove channel[{}][{}] from ProducerManager groupChannelTable, producer group: {}",
                                        clientChannelInfo.toString(), remoteAddr, group);
                            }

                        }
                    } finally {
                        this.groupChannelLock.unlock();
                    }
                } else {
                    log.warn("ProducerManager doChannelCloseEvent lock timeout");
                }
            } catch (InterruptedException e) {
                log.error("", e);
            }
        }
    }

    public void registerProducer(final String group, final ClientChannelInfo clientChannelInfo) {

        try {
            ClientChannelInfo clientChannelInfoFound = null;
            if (this.hashcodeChannelLock.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                try {
                    List<ClientChannelInfo> clientChannelInfoList =
                            this.hashcodeChannelTable.get(group.hashCode());
                    if (null == clientChannelInfoList) {
                        clientChannelInfoList = new ArrayList<ClientChannelInfo>();
                        this.hashcodeChannelTable.put(group.hashCode(), clientChannelInfoList);
                    }

                    int index = clientChannelInfoList.indexOf(clientChannelInfo);
                    if (index >= 0) {
                        clientChannelInfoFound = clientChannelInfoList.get(index);
                    }

                    if (null == clientChannelInfoFound) {
                        clientChannelInfoList.add(clientChannelInfo);
                    }
                } finally {
                    this.hashcodeChannelLock.unlock();
                }

                if (clientChannelInfoFound != null) {
                    clientChannelInfoFound.setLastUpdateTimestamp(System.currentTimeMillis());
                }
            } else {
                log.warn("ProducerManager registerProducer lock timeout");
            }
        } catch (InterruptedException e) {
            log.error("", e);
        }

        try {
            ClientChannelInfo clientChannelInfoFound = null;

            if (this.groupChannelLock.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                try {
                    HashMap<Channel, ClientChannelInfo> channelTable = this.groupChannelTable.get(group);
                    if (null == channelTable) {
                        channelTable = new HashMap<>();
                        this.groupChannelTable.put(group, channelTable);
                    }

                    clientChannelInfoFound = channelTable.get(clientChannelInfo.getChannel());
                    if (null == clientChannelInfoFound) {
                        channelTable.put(clientChannelInfo.getChannel(), clientChannelInfo);
                        log.info("new producer connected, group: {} channel: {}", group,
                                clientChannelInfo.toString());
                    }
                } finally {
                    this.groupChannelLock.unlock();
                }

                if (clientChannelInfoFound != null) {
                    clientChannelInfoFound.setLastUpdateTimestamp(System.currentTimeMillis());
                }
            } else {
                log.warn("ProducerManager registerProducer lock timeout");
            }
        } catch (InterruptedException e) {
            log.error("", e);
        }
    }

    public void unregisterProducer(final String group, final ClientChannelInfo clientChannelInfo) {

        try {
            if (this.hashcodeChannelLock.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                try {
                    List<ClientChannelInfo> clientChannelInfoList =
                            this.hashcodeChannelTable.get(group.hashCode());
                    if (null != clientChannelInfoList && !clientChannelInfoList.isEmpty()) {
                        boolean result = clientChannelInfoList.remove(clientChannelInfo);
                        if (result) {
                            log.info("unregister a producer[{}] from hashcodeChannelTable {}", group,
                                    clientChannelInfo.toString());
                        }

                        if (clientChannelInfoList.isEmpty()) {
                            this.hashcodeChannelTable.remove(group.hashCode());
                            log.info("unregister a producer group[{}] from hashcodeChannelTable", group);
                        }
                    }
                } finally {
                    this.hashcodeChannelLock.unlock();
                }
            } else {
                log.warn("ProducerManager unregisterProducer lock timeout");
            }
        } catch (InterruptedException e) {
            log.error("", e);
        }

        try {
            if (this.groupChannelLock.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                try {
                    HashMap<Channel, ClientChannelInfo> channelTable = this.groupChannelTable.get(group);
                    if (null != channelTable && !channelTable.isEmpty()) {
                        ClientChannelInfo old = channelTable.remove(clientChannelInfo.getChannel());
                        if (old != null) {
                            log.info("unregister a producer[{}] from groupChannelTable {}", group,
                                    clientChannelInfo.toString());
                        }

                        if (channelTable.isEmpty()) {
                            this.groupChannelTable.remove(group);
                            log.info("unregister a producer group[{}] from groupChannelTable", group);
                        }
                    }
                } finally {
                    this.groupChannelLock.unlock();
                }
            } else {
                log.warn("ProducerManager unregisterProducer lock timeout");
            }
        } catch (InterruptedException e) {
            log.error("", e);
        }
    }

    public ClientChannelInfo pickProducerChannelRandomly(final int producerGroupHashCode) {
        try {
            if (this.groupChannelLock.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                try {
                    List<ClientChannelInfo> channelInfoList =
                            this.hashcodeChannelTable.get(producerGroupHashCode);
                    if (channelInfoList != null && !channelInfoList.isEmpty()) {
                        int index = this.generateRandmonNum() % channelInfoList.size();
                        ClientChannelInfo info = channelInfoList.get(index);
                        return info;
                    }
                } finally {
                    this.groupChannelLock.unlock();
                }
            } else {
                log.warn("ProducerManager pickProducerChannelRandomly lock timeout");
            }
        } catch (InterruptedException e) {
            log.error("", e);
        }
        return null;
    }

    private int generateRandmonNum() {
        int value = this.random.nextInt();

        if (value < 0) {
            value = Math.abs(value);
        }
        return value;
    }
}
