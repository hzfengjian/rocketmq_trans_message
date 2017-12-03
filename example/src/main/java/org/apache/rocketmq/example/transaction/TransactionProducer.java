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
package org.apache.rocketmq.example.transaction;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.TransactionCheckListener;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.PropertyKeyConst;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class TransactionProducer {

    public static Map<Integer, Boolean> statsMap = new ConcurrentHashMap<>();

    public static void main(String[] args) throws MQClientException, InterruptedException {
        TransactionCheckListener transactionCheckListener = new TransactionCheckListenerImpl();
        TransactionMQProducer producer = new TransactionMQProducer("please_rename_unique_group_name");
        producer.setCheckThreadPoolMinSize(2);
        producer.setCheckThreadPoolMaxSize(2);
        producer.setCheckRequestHoldMax(2000);
        //producer.setNamesrvAddr("127.0.0.1:9876");
         producer.setNamesrvAddr("testserver004:9876");
        producer.setTransactionCheckListener(transactionCheckListener);
        producer.start();


        String[] tags = new String[]{"TagA", "TagB", "TagC", "TagD", "TagE"};
        TransactionExecuterImpl tranExecuter = new TransactionExecuterImpl();
        ExecutorService executorService = Executors.newFixedThreadPool(20);
        for (int j = 0; j < 5000; j++) {
            final int i = j;
            executorService.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        int xx = i;
                        Message msg =
                                new Message("TransactionTest", tags[i % tags.length], "KEY" + i,
                                        String.valueOf(xx).getBytes(RemotingHelper.DEFAULT_CHARSET));
                        long startTime = System.currentTimeMillis();
                        msg.putUserProperty(PropertyKeyConst.CheckImmunityTimeInSeconds, "30");
                        //producer.send(msg);
                        SendResult sendResult = producer.sendMessageInTransaction(msg, tranExecuter, i);
                        //System.out.println("发送消息:" + sendResult + ",消耗时间:" + (System.currentTimeMillis() - startTime) + "ms");
                        System.out.println("发送消息:" + xx + ",消耗时间:" + (System.currentTimeMillis() - startTime) + "ms");
                        statsMap.put(i, true);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            });
        }

        for (int i = 0; i < 100000; i++) {
            Thread.sleep(1000);
        }
        producer.shutdown();
    }
}
