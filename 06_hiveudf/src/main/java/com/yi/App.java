package com.yi;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

/**
 * Hive自定义函数
 * 将小写字母转换为大写
 *
 * @author huangwenyi
 * @date 2019-8-26
 */
public class App extends UDF {
    public Text evaluate(final Text s) {
        if (null == s) {
            return null;
        }
        //返回大写字母
        return new Text(s.toString().toUpperCase());
    }

}
