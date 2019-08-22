package com.yi.demo2.sort;

import com.yi.demo2.sort.entity.Sort;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 要求第一列按照字典顺序进行排列，第一列相同的时候，第二列按照升序进行排列
 *
 * @author huangwenyi
 * @date 2019-8-22
 */
public class PairSort implements WritableComparable<PairSort> {
    private Sort sort;
    {
        sort = new Sort();
    }

    /**
     * 比较器
     *
     * @param o
     * @return
     */
    @Override
    public int compareTo(PairSort o) {
        //比较我们第一列的数据
        int compareTo = this.sort.getFirst().compareTo(o.sort.getFirst());
        // 不等于0 说明第一列比较的两个数不相等
        if (compareTo != 0) {
            return compareTo;
        } else {
            // 如果第一列相等 就比较第二列，默认按照升序排序，需要降序可以使用负数
            return -this.sort.getSecond().compareTo(o.sort.getSecond());
        }
    }

    /**
     * 序列化
     *
     * @param out 输出源
     * @throws IOException
     */
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(sort.getFirst());
        out.writeInt(sort.getSecond());
    }

    /**
     * 反序列化
     *
     * @param in 输入源
     * @throws IOException
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        sort.setFirst(in.readUTF());
        sort.setSecond(in.readInt());
    }

    public Sort getSort() {
        return sort;
    }

    public void setSort(Sort sort) {
        this.sort = sort;
    }

    @Override
    public String toString() {
        return "PairSort{" +
                "sort=" + sort +
                '}';
    }
}
