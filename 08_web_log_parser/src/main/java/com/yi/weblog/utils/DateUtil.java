package com.yi.weblog.utils;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class DateUtil  {

    /**
     * 获取昨日的日期
     * @return
     */
    public static String getYestDate(){
        Calendar instance = Calendar.getInstance();
        instance.add(Calendar.DATE,-1);
        Date time = instance.getTime();
        String format = new SimpleDateFormat("yyyy-MM-dd").format(time);
        return format;
    }
    public static void main(String[] args) {
        getYestDate();
    }
}
