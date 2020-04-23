package com.github.quartz.impl.redisjobstore.delegate;

import org.quartz.impl.jdbcjobstore.StdJDBCConstants;
import org.quartz.impl.triggers.CalendarIntervalTriggerImpl;
import org.quartz.spi.OperableTrigger;

public class CalendarIntervalTriggerTypeDelegate implements TriggerTypeDelegate, StdJDBCConstants {

    @Override
    public boolean canHandleTriggerType(OperableTrigger trigger) {
        return ((trigger instanceof CalendarIntervalTriggerImpl) && !((CalendarIntervalTriggerImpl)trigger).hasAdditionalProperties());
    }

    @Override
    public String getHandledTriggerTypeDiscriminator() {
        return TTYPE_CAL_INT;
    }

    @Override
    public Class<? extends OperableTrigger> getTriggerClass() {
        return CalendarIntervalTriggerImpl.class;
    }

}
