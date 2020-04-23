package com.github.quartz.impl.redisjobstore;

import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;

import java.util.Properties;

public class MainUsingDb1 {

    public static void main(String[] args) throws Exception {
        Properties properties = Util.getJdbcProperties();
        SchedulerFactory sf = new StdSchedulerFactory(properties);
        Scheduler scheduler = sf.getScheduler();
        //scheduler.clear();

        JobKey jobKey = new JobKey("job1", "group1");
        TriggerKey triggerKey = new TriggerKey("trigger1", "group1");
        JobDetail jobDetail = JobBuilder.newJob(HelloJob.class)
                .withIdentity(jobKey)
                .usingJobData("name", "quartz")
                .usingJobData("pi", 3.1415926)
                .withDescription("测试")
                .storeDurably(true)
                .requestRecovery(true)
                .build();
        CronTrigger trigger = TriggerBuilder.newTrigger().withIdentity(triggerKey)
                .withSchedule(CronScheduleBuilder.cronSchedule("0/5 * * * * ?").withMisfireHandlingInstructionDoNothing())
                .withDescription("cron表达式触发器")
                .build();
        scheduler.scheduleJob(jobDetail, trigger);
        scheduler.start();
    }

}
