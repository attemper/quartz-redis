package com.github.quartz.impl.redisjobstore.constant;

import org.quartz.impl.jdbcjobstore.Constants;

public interface RedisConstants extends Constants {

    /**
     * job detail
     * 0 schedName
     * 1 jobName
     * 2 groupName
     */
    String KEY_JOB = "JOB@{0}#{1}${2}";

    /**
     * jobs set
     * 0 schedName
     */
    String KEY_JOBS = "JOBS@{0}";

    /**
     * trigger info
     * 0 schedName
     * 1 triggerName
     * 2 groupName
     */
    String KEY_TRIGGER = "TRIGGER@{0}#{1}${2}";

    /**
     * triggers set
     * 0 schedName
     */
    String KEY_TRIGGERS = "TRIGGERS@{0}";

    /**
     * waiting triggers zset
     * 0 schedName
     */
    String KEY_WAITING_STATE_TRIGGERS = "WAITING_STATE_TRIGGERS@{0}";

    /**
     * acquired/executing/complete/blocked/error/paused/paused blocked triggers set
     * 0 schedName
     */
    String KEY_OTHER_STATE_TRIGGERS = "{0}_STATE_TRIGGERS@{1}";

    /**
     * job triggers set
     * 0 schedName
     * 1 jobName
     * 2 groupName
     * value triggerName groupName
     */
    String KEY_JOB_TRIGGERS = "JOB_TRIGGERS@{0}#{1}${2}";

    /**
     * fired instances set
     * 0 schedName
     * value instanceId
     */
    String KEY_FIRED_INSTANCES = "FIRED_INSTANCES@{0}";

    /**
     * fired instance's jobs set
     * 0 schedName
     * 1 instanceId
     * value entryId
     */
    String KEY_FIRED_JOBS = "FIRED_JOBS@{0}#{1}";

    /**
     * hash
     * 0 schedName
     * 1 entryId
     * value fired job info
     */
    String KEY_FIRED_JOB = "FIRED_JOB@{0}#{1}";

    /**
     * set
     * 0 schedName
     * value groupName
     */
    String KEY_PAUSED_TRIGGER_GROUPS = "PAUSED_TRIGGER_GROUPS@{0}";

    /**
     * hash
     * 0 schedName
     * 1 calendarName
     * value calendar info
     */
    String KEY_CALENDAR = "CALENDAR@{0}#{1}";

    /**
     * set
     * 0 schedName
     * value calendarName
     */
    String KEY_CALENDARS = "CALENDARS@{0}";

    /**
     * set
     * 0 schedName
     * 1 calendarName
     * value triggerName groupName
     */
    String KEY_CALENDAR_TRIGGERS = "CALENDAR_TRIGGERS@{0}#{1}";

    /**
     * set
     * 0 schedName
     * value instanceId
     */
    String KEY_SCHEDULER_STATES = "SCHEDULER_STATES@{0}";

    /**
     * hash
     * 0 schedName
     * 1 instanceId
     */
    String KEY_SCHEDULER_STATE = "SCHEDULER_STATE@{0}#{1}";

    String VALUE_DELIMITER = "@@@";
}
