package com.github.quartz.impl.redisjobstore;

import org.quartz.Scheduler;
import org.quartz.SchedulerFactory;
import org.quartz.impl.StdSchedulerFactory;

import java.util.Properties;

public class MainUsingRedis {

    public static void main(String[] args) throws Exception {
        Properties properties = Util.getRedisProperties();
        SchedulerFactory sf = new StdSchedulerFactory(properties);
        Scheduler scheduler = sf.getScheduler();
        //scheduler.clear();
        scheduler.start();
    }

}
