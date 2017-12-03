package org.apache.rocketmq.common.utils;

/**
 * @author fengjian
 * @version V1.0
 * @title: rocketmq-all
 * @Package org.apache.rocketmq.common.utils
 * @Description:
 * @date 2017/11/8 上午10:34
 */
public class ThreadUtils {

    public static void sleep(long mills) {
        try {
            Thread.sleep(mills);
        } catch (InterruptedException e) {
        }
    }
}
