package com.github.quartz.impl.redisjobstore.constant;

public interface FieldConstants {

    // job
    String FIELD_JOB_NAME = "jobName";

    String FIELD_JOB_GROUP = "jobGroup";

    String FIELD_JOB_DATA_MAP = "jobDataMap";

    String FIELD_DURABLE = "durable";

    String FIELD_REQUESTS_RECOVERY = "requestsRecovery";

    // trigger
    String FIELD_TRIGGER_NAME = "triggerName";

    String FIELD_TRIGGER_GROUP = "triggerGroup";

    String FIELD_STATE = "state";

    String FIELD_TYPE = "type";

    String FIELD_START_TIME = "startTime";

    String FIELD_END_TIME = "endTime";

    String FIELD_PREV_FIRE_TIME  = "prevFireTime";

    String FIELD_NEXT_FIRE_TIME = "nextFireTime";

    String FIELD_MISFIRE_INSTRUCTION = "misfireInstruction";

    String FIELD_PRIORITY = "priority";

    // cron trigger
    String FIELD_CRON_EXPRESSION = "cronExpression";


    // fired record
    String FIELD_ENTRY_ID = "entryId";

    String FIELD_INSTANCE_ID = "instanceId";

    String FIELD_FIRED_TIME = "firedTime";

    String FIELD_SCHED_TIME = "schedTime";

    String FIELD_LAST_CHECKIN_TIME = "lastCheckinTime";

    String FIELD_CHECKIN_INTERVAL = "checkinInterval";
}
