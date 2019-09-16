package com.yi.readboard;

import org.junit.Test;

public class DemoTest {

    @Test
    public void test1(){
        String arr = "'BTC' and 1=1";
        String sql = "select * from information.coin_info where coin_name a= %s";

        sql = String.format(sql, arr);

        System.out.println(sql);
    }
}
