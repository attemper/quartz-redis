package com.github.quartz.impl.redisjobstore.mixin;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.github.quartz.impl.redisjobstore.jackson.IntegerSetDeserializer;
import com.github.quartz.impl.redisjobstore.jackson.IntegerSetSerializer;
import com.github.quartz.impl.redisjobstore.jackson.TimeOfDayDeserializer;
import com.github.quartz.impl.redisjobstore.jackson.TimeOfDaySerializer;
import org.quartz.TimeOfDay;

import java.util.Set;

public abstract class DailyTimeIntervalTriggerMixIn extends TriggerMixIn {

    @JsonDeserialize(using = TimeOfDayDeserializer.class)
    public abstract TimeOfDay getStartTimeOfDay();

    @JsonSerialize(using = TimeOfDaySerializer.class)
    public abstract void setStartTimeOfDay(TimeOfDay startTimeOfDay);

    @JsonDeserialize(using = TimeOfDayDeserializer.class)
    public abstract TimeOfDay getEndTimeOfDay();

    @JsonSerialize(using = TimeOfDaySerializer.class)
    public abstract void setEndTimeOfDay(TimeOfDay endTimeOfDay);

    @JsonDeserialize(using = IntegerSetDeserializer.class)
    public abstract Set<Integer> getDaysOfWeek();

    @JsonSerialize(using = IntegerSetSerializer.class)
    public abstract void setDaysOfWeek(Set<Integer> daysOfWeek);
}
