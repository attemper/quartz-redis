package com.github.quartz.impl.redisjobstore.delegate;

import org.quartz.CalendarOffsetTrigger;
import org.quartz.impl.jdbcjobstore.StdJDBCConstants;
import org.quartz.impl.triggers.CalendarOffsetTriggerImpl;
import org.quartz.spi.OperableTrigger;

public class CalendarOffsetTriggerTypeDelegate implements TriggerTypeDelegate, StdJDBCConstants {

    @Override
    public boolean canHandleTriggerType(OperableTrigger trigger) {
        return ((trigger instanceof CalendarOffsetTrigger) && !((CalendarOffsetTriggerImpl)trigger).hasAdditionalProperties());
    }

    @Override
    public String getHandledTriggerTypeDiscriminator() {
        return TTYPE_CAL_OFFSET;
    }

    @Override
    public Class<? extends OperableTrigger> getTriggerClass() {
        return CalendarOffsetTriggerImpl.class;
    }
}
