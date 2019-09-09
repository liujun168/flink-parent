package com.hztq.sc.flink.demo.bk;

import com.sun.management.OperatingSystemMXBean;

import java.lang.management.ManagementFactory;

/**
 * @description: MemoryUsageExtrator 很简单的工具类，提取当前可用内存字节数
 * @author: liujun 249489478@qq.com
 * @create: 2019-09-09 15:11
 */
public class MemoryUsageExtrator {

    private static OperatingSystemMXBean mxBean =
            (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();

    /**
     * Get current free memory size in bytes
     *
     * @return free RAM size
     */
    public static long currentFreeMemorySizeInBytes() {
        return mxBean.getFreePhysicalMemorySize();
    }
}
