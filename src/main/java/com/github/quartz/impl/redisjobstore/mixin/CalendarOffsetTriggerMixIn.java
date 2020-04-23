package com.github.quartz.impl.redisjobstore.mixin;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.github.quartz.impl.redisjobstore.jackson.TimeOfDayDeserializer;
import com.github.quartz.impl.redisjobstore.jackson.TimeOfDaySerializer;
import org.quartz.TimeOfDay;

public abstract class CalendarOffsetTriggerMixIn extends TriggerMixIn {

    @JsonDeserialize(using = TimeOfDayDeserializer.class)
    public abstract TimeOfDay getStartTimeOfDay();

    @JsonSerialize(using = TimeOfDaySerializer.class)
    public abstract void setStartTimeOfDay(TimeOfDay startTimeOfDay);

}
