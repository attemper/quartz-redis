package com.github.quartz.impl.redisjobstore.mixin;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.github.quartz.impl.redisjobstore.constant.FieldConstants;
import org.quartz.CronExpression;

public abstract class CronTriggerMixIn extends TriggerMixIn implements FieldConstants {
    @JsonIgnore
    public abstract String getExpressionSummary();

    @JsonIgnore
    public abstract void setCronExpression(CronExpression cron);

    @JsonProperty(FIELD_CRON_EXPRESSION)
    public abstract void setCronExpression(String cronExpression);

    @JsonProperty(FIELD_CRON_EXPRESSION)
    public abstract String getCronExpression();

}
