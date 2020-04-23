package com.github.quartz.impl.redisjobstore.mixin;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.github.quartz.impl.redisjobstore.constant.FieldConstants;
import com.github.quartz.impl.redisjobstore.jackson.ObjectDeserializer;
import com.github.quartz.impl.redisjobstore.jackson.ObjectSerializer;
import org.quartz.*;

import java.util.Date;

public abstract class TriggerMixIn implements FieldConstants {

    @JsonIgnore
    public abstract TriggerKey getKey();

    @JsonIgnore
    public abstract JobKey getJobKey();

    @JsonSerialize(using = ObjectSerializer.class)
    public abstract void setJobDataMap(JobDataMap jobDataMap);

    @JsonDeserialize(using = ObjectDeserializer.class)
    public abstract JobDataMap getJobDataMap();

    @JsonIgnore
    public abstract boolean mayFireAgain();

    @JsonProperty(FIELD_START_TIME)
    public abstract void setStartTime(Date startTime);

    @JsonProperty(FIELD_START_TIME)
    public abstract Date getStartTime();

    @JsonProperty(FIELD_END_TIME)
    public abstract void setEndTime(Date startTime);

    @JsonProperty(FIELD_END_TIME)
    public abstract Date getEndTime();

    @JsonProperty(FIELD_PREV_FIRE_TIME)
    public abstract Date getPreviousFireTime();

    @JsonIgnore
    public abstract Date getFinalFireTime();

    @JsonIgnore
    public abstract String getFullName();

    @JsonIgnore
    public abstract String getFullJobName();

    @JsonIgnore
    public abstract TriggerBuilder getTriggerBuilder();

    @JsonIgnore
    public abstract ScheduleBuilder getScheduleBuilder();

}
