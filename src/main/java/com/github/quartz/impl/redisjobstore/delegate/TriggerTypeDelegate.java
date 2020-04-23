package com.github.quartz.impl.redisjobstore.delegate;

import org.quartz.spi.OperableTrigger;

public interface TriggerTypeDelegate {

    boolean canHandleTriggerType(OperableTrigger trigger);

    String getHandledTriggerTypeDiscriminator();

    Class<? extends OperableTrigger> getTriggerClass();

}
