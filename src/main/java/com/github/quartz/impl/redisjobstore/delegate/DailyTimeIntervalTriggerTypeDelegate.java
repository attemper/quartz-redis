package com.github.quartz.impl.redisjobstore.delegate;

import org.quartz.DailyTimeIntervalTrigger;
import org.quartz.impl.jdbcjobstore.StdJDBCConstants;
import org.quartz.impl.triggers.DailyTimeIntervalTriggerImpl;
import org.quartz.spi.OperableTrigger;

public class DailyTimeIntervalTriggerTypeDelegate implements TriggerTypeDelegate, StdJDBCConstants {

    @Override
    public boolean canHandleTriggerType(OperableTrigger trigger) {
        return ((trigger instanceof DailyTimeIntervalTrigger) && !((DailyTimeIntervalTriggerImpl)trigger).hasAdditionalProperties());
    }

    @Override
    public String getHandledTriggerTypeDiscriminator() {
        return TTYPE_DAILY_TIME_INT;
    }

    @Override
    public Class<? extends OperableTrigger> getTriggerClass() {
        return DailyTimeIntervalTriggerImpl.class;
    }
}
