package com.github.quartz.impl.redisjobstore;

import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;

import java.util.Properties;

public class MainUsingRedis {

    public static void main(String[] args) throws Exception {
        Properties properties = Util.getRedisProperties();
        SchedulerFactory sf = new StdSchedulerFactory(properties);
        Scheduler scheduler = sf.getScheduler();
        //scheduler.clear();
        //s1(scheduler);
        //s2(scheduler);
        //s3(scheduler);
        scheduler.start();
    }

    public static void s1(Scheduler scheduler) throws SchedulerException {
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
                .withPriority(10)
                .build();
        scheduler.scheduleJob(jobDetail, trigger);
    }

    public static void s2(Scheduler scheduler) throws SchedulerException {
        JobKey jobKey = new JobKey("job2", "group1");
        TriggerKey triggerKey = new TriggerKey("trigger2", "group1");
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
                .withPriority(20)
                .build();
        scheduler.scheduleJob(jobDetail, trigger);
    }

    public static void s3(Scheduler scheduler) throws SchedulerException {
        JobKey jobKey = new JobKey("job3", "group1");
        TriggerKey triggerKey = new TriggerKey("trigger3", "group1");
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
                .withPriority(30)
                .build();
        scheduler.scheduleJob(jobDetail, trigger);
    }
}
