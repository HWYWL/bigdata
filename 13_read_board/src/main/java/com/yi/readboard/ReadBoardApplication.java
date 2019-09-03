package com.yi.readboard;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * bin/storm jar /export/servers/read-board-0.0.1-SNAPSHOT.jar com.yi.readboard.ReadBoardApplication SpringbootStorm
 * 实时看板
 *
 * @author huangwenyi
 * @date 2019-9-3
 */
@SpringBootApplication
public class ReadBoardApplication {

    public static void main(String[] args) {
        SpringApplication app = new SpringApplication(ReadBoardApplication.class);
        //不需要web servlet功能，所以设置为WebApplicationType.NONE
        app.setWebApplicationType(WebApplicationType.NONE);
        app.run(args);
    }
}
