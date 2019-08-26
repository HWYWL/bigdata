package com.yi.iterceptor;

import com.google.common.base.Charsets;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.yi.iterceptor.CustomParameterInterceptor.Constants.*;

public class CustomParameterInterceptor implements Interceptor {
    /**
     * The field_separator.指明每一行字段的分隔符
     */
    private final String fields_separator;

    /**
     * The indexs.通过分隔符分割后，指明需要那列的字段 下标
     */
    private final String indexs;

    /**
     * The indexs_separator. 多个下标的分隔符
     */
    private final String indexs_separator;

    /**
     * The encrypted_field_index. 需要加密的字段下标
     */
    private final String encrypted_field_index;

    /**
     * @param indexs
     * @param indexsSeparator
     */
    public CustomParameterInterceptor(String fieldsSeparator,
                                      String indexs, String indexsSeparator, String encryptedFieldIndex) {
        String f = fieldsSeparator.trim();
        String i = indexsSeparator.trim();
        this.indexs = indexs;
        this.encrypted_field_index = encryptedFieldIndex.trim();
        if (!"".equals(f)) {
            f = unicodeToString(f);
        }
        this.fields_separator = f;
        if (!"".equals(i)) {
            i = unicodeToString(i);
        }
        this.indexs_separator = i;
    }

    @Override
    public void initialize() {

    }

    @Override
    public Event intercept(Event event) {
        if (event == null) {
            return null;
        }
        try {
            //获取我们的一行数据
            String line = new String(event.getBody(), Charsets.UTF_8);
            String[] fieldsSpilts = line.split(fields_separator);
            // indexs 也是我们从外部传入进来的，表示我们需要获取哪些下标的字段数据
            String[] indexsSplit = indexs.split(indexs_separator);
            StringBuilder newLine = new StringBuilder();
            for (int i = 0; i < indexsSplit.length; i++) {
                int parseInt = Integer.parseInt(indexsSplit[i]);
                //对加密字段进行加密
                if (!"".equals(encrypted_field_index) && encrypted_field_index.equals(indexsSplit[i])) {
                    newLine.append(StringUtils.getMD5Code(fieldsSpilts[parseInt]));
                } else {
                    newLine.append(fieldsSpilts[parseInt]);
                }

                if (i != indexsSplit.length - 1) {
                    newLine.append(fields_separator);
                }
            }
            event.setBody(newLine.toString().getBytes(Charsets.UTF_8));
            return event;
        } catch (Exception e) {
            return event;
        }
    }

    @Override
    public List<Event> intercept(List<Event> events) {
        List<Event> out = new ArrayList<>();
        for (Event event : events) {
            Event outEvent = intercept(event);
            if (outEvent != null) {
                out.add(outEvent);
            }
        }
        return out;
    }

    @Override
    public void close() {

    }

    /**
     * 相当于自定义Interceptor的工厂类
     * 在flume采集配置文件中通过制定该Builder来创建Interceptor对象
     * 可以在Builder中获取、解析flume采集配置文件中的拦截器Interceptor的自定义参数：
     * 字段分隔符，字段下标，下标分隔符、加密字段下标 ...等
     *
     * @author
     */
    public static class Builder implements Interceptor.Builder {

        /**
         * The fields_separator.指明每一行字段的分隔符
         */
        private String fieldsSeparator;

        /**
         * The indexs.通过分隔符分割后，指明需要那列的字段 下标
         */
        private String indexs;

        /**
         * The indexs_separator. 多个下标下标的分隔符
         */
        private String indexsSeparator;

        /**
         * The encrypted_field. 需要加密的字段下标
         */
        private String encryptedFieldIndex;

        /*
         * @see org.apache.flume.conf.Configurable#configure(org.apache.flume.Context)
         */
        @Override
        public void configure(Context context) {
            fieldsSeparator = context.getString(FIELD_SEPARATOR, DEFAULT_FIELD_SEPARATOR);
            indexs = context.getString(INDEXS, DEFAULT_INDEXS);
            indexsSeparator = context.getString(INDEXS_SEPARATOR, DEFAULT_INDEXS_SEPARATOR);
            encryptedFieldIndex = context.getString(ENCRYPTED_FIELD_INDEX, DEFAULT_ENCRYPTED_FIELD_INDEX);
        }

        /*
         * @see org.apache.flume.interceptor.Interceptor.Builder#build()
         */
        @Override
        public Interceptor build() {
            return new CustomParameterInterceptor(fieldsSeparator, indexs, indexsSeparator, encryptedFieldIndex);
        }
    }

    /**
     * 常量
     */
    public static class Constants {
        /**
         * The Constant FIELD_SEPARATOR.
         */
        static final String FIELD_SEPARATOR = "fields_separator";

        /**
         * The Constant DEFAULT_FIELD_SEPARATOR.
         */
        static final String DEFAULT_FIELD_SEPARATOR = " ";

        /**
         * The Constant INDEXS.
         */
        static final String INDEXS = "indexs";

        /**
         * The Constant DEFAULT_INDEXS.
         */
        static final String DEFAULT_INDEXS = "0";

        /**
         * The Constant INDEXS_SEPARATOR.
         */
        static final String INDEXS_SEPARATOR = "indexs_separator";

        /**
         * The Constant DEFAULT_INDEXS_SEPARATOR.
         */
        static final String DEFAULT_INDEXS_SEPARATOR = ",";

        /**
         * The Constant ENCRYPTED_FIELD_INDEX.
         */
        static final String ENCRYPTED_FIELD_INDEX = "encrypted_field_index";

        /**
         * The Constant DEFAUL_TENCRYPTED_FIELD_INDEX.
         */
        static final String DEFAULT_ENCRYPTED_FIELD_INDEX = "";

        /**
         * The Constant PROCESSTIME.
         */
        public static final String PROCESSTIME = "processTime";
        /**
         * The Constant PROCESSTIME.
         */
        public static final String DEFAULT_PROCESSTIME = "a";

    }

    /**
     * 工具类：字符串md5加密
     */
    public static class StringUtils {
        // 全局数组
        private final static String[] STR_DIGITS = {"0", "1", "2", "3", "4", "5",
                "6", "7", "8", "9", "a", "b", "c", "d", "e", "f"};

        // 返回形式为数字跟字符串
        private static String byteToArrayString(byte bByte) {
            int iRet = bByte;
            // System.out.println("iRet="+iRet);
            if (iRet < 0) {
                iRet += 256;
            }
            int iD1 = iRet / 16;
            int iD2 = iRet % 16;
            return STR_DIGITS[iD1] + STR_DIGITS[iD2];
        }

        // 返回形式只为数字
        private static String byteToNum(byte bByte) {
            int iRet = bByte;
            System.out.println("iRet1=" + iRet);
            if (iRet < 0) {
                iRet += 256;
            }
            return String.valueOf(iRet);
        }

        // 转换字节数组为16进制字串
        private static String byteToString(byte[] bByte) {
            StringBuilder sBuffer = new StringBuilder();
            for (int i = 0; i < bByte.length; i++) {
                sBuffer.append(byteToArrayString(bByte[i]));
            }
            return sBuffer.toString();
        }

        public static String getMD5Code(String strObj) {
            String resultString = null;
            try {
                resultString = new String(strObj);
                MessageDigest md = MessageDigest.getInstance("MD5");
                // md.digest() 该函数返回值为存放哈希值结果的byte数组
                resultString = byteToString(md.digest(strObj.getBytes()));
            } catch (NoSuchAlgorithmException ex) {
                ex.printStackTrace();
            }
            return resultString;
        }
    }

    /**
     * \t 制表符 ('\u0009') \n 新行（换行）符 (' ') \r 回车符 (' ') \f 换页符 ('\u000C') \a 报警
     * (bell) 符 ('\u0007') \e 转义符 ('\u001B') \cx  空格(\u0020)对应于 x 的控制符
     *
     * @param str
     * @return
     * @data:2015-6-30
     */
    public static String unicodeToString(String str) {
        Pattern pattern = Pattern.compile("(\\\\u(\\p{XDigit}{4}))");
        Matcher matcher = pattern.matcher(str);
        char ch;
        while (matcher.find()) {
            ch = (char) Integer.parseInt(matcher.group(2), 16);
            str = str.replace(matcher.group(1), ch + "");
        }
        return str;
    }
}
