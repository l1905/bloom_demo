package org.example;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class Main {
    // 读取配置文件
    private static InputStream getFileInputStream(String argEnv) {
        String filename = "config.properties";
//        log.info("加载配置文件 {}", filename);
        return Main.class.getClassLoader().getResourceAsStream(filename);
    }

    public static void main(String[] args) throws IOException {
//        System.out.println("hello bloom demo");
        // 加载配置文件
        String argEnv = "";
        if (args != null && args.length >= 1) {
            argEnv = args[0];
        }
        Properties properties = new Properties();
        properties.load(getFileInputStream(argEnv));


        BloomTest bloomTest = new BloomTest();
        bloomTest.initConfig(properties);

        // 每次运行，需flushall清空redis

        // 所有的浏览记录，共用1个bloomfilter对象
//        bloomTest.initData01();
//        bloomTest.check01();

//        bloomTest.initData02();
//        bloomTest.check02();

        bloomTest.initData03();
//        bloomTest.check03();

    }
}
