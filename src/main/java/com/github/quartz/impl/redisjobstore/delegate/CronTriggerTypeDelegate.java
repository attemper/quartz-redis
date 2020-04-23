package com.github.quartz.impl.redisjobstore.delegate;

import org.quartz.impl.jdbcjobstore.StdJDBCConstants;
import org.quartz.impl.triggers.CronTriggerImpl;
import org.quartz.spi.OperableTrigger;

public class CronTriggerTypeDelegate implements TriggerTypeDelegate, StdJDBCConstants {

    @Override
    public boolean canHandleTriggerType(OperableTrigger trigger) {
        return ((trigger instanceof CronTriggerImpl) && !((CronTriggerImpl)trigger).hasAdditionalProperties());
    }

    @Override
    public String getHandledTriggerTypeDiscriminator() {
        return TTYPE_CRON;
    }

    @Override
    public Class<? extends OperableTrigger> getTriggerClass() {
        return CronTriggerImpl.class;
    }
}
