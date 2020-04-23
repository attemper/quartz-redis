package com.github.quartz.impl.redisjobstore.delegate;

import org.quartz.impl.jdbcjobstore.StdJDBCConstants;
import org.quartz.impl.triggers.SimpleTriggerImpl;
import org.quartz.spi.OperableTrigger;

public class SimpleTriggerTypeDelegate implements TriggerTypeDelegate, StdJDBCConstants {

    @Override
    public boolean canHandleTriggerType(OperableTrigger trigger) {
        return ((trigger instanceof SimpleTriggerImpl) && !((SimpleTriggerImpl)trigger).hasAdditionalProperties());
    }

    @Override
    public String getHandledTriggerTypeDiscriminator() {
        return TTYPE_SIMPLE;
    }

    @Override
    public Class<? extends OperableTrigger> getTriggerClass() {
        return SimpleTriggerImpl.class;
    }
}
