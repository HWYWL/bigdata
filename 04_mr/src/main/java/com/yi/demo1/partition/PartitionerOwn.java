package com.yi.demo1.partition;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 自定义分区
 * Partitioner 泛型和 Map阶段输出类型一致
 *
 * @author huangwenyi
 * @date 2019-8-21
 */
public class PartitionerOwn extends Partitioner<Text, NullWritable> {

    /**
     * @param text          k2
     * @param nullWritable  v2
     * @param numPartitions 分区总数
     * @return
     */
    @Override
    public int getPartition(Text text, NullWritable nullWritable, int numPartitions) {
        // 按照制表符分割数据
        String[] split = text.toString().split("\t");
        String gameResult = split[5];
        if (StringUtils.isNotEmpty(gameResult)){
            //第五个字段表示开奖结果数值，现在需求将15以上的结果以及15以下的结果进行分开成两个文件进行保存
            return Integer.parseInt(gameResult) > 15 ? 0 : 1;
        }

        return 0;
    }
}
