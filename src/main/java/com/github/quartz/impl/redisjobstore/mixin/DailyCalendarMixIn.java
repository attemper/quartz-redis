package com.github.quartz.impl.redisjobstore.mixin;

import com.fasterxml.jackson.annotation.JsonProperty;

public abstract class DailyCalendarMixIn {

    @JsonProperty("rangeStartingHourOfDay")
    private int rangeStartingHourOfDay;

    @JsonProperty("rangeStartingMinute")
    private int rangeStartingMinute;

    @JsonProperty("rangeStartingSecond")
    private int rangeStartingSecond;

    @JsonProperty("rangeStartingMillis")
    private int rangeStartingMillis;

    @JsonProperty("rangeEndingHourOfDay")
    private int rangeEndingHourOfDay;

    @JsonProperty("rangeEndingMinute")
    private int rangeEndingMinute;

    @JsonProperty("rangeEndingSecond")
    private int rangeEndingSecond;

    @JsonProperty("rangeEndingMillis")
    private int rangeEndingMillis;

    public void setRangeStartingHourOfDay(int rangeStartingHourOfDay) {
        this.rangeStartingHourOfDay = rangeStartingHourOfDay;
    }

    public void setRangeStartingMinute(int rangeStartingMinute) {
        this.rangeStartingMinute = rangeStartingMinute;
    }

    public void setRangeStartingSecond(int rangeStartingSecond) {
        this.rangeStartingSecond = rangeStartingSecond;
    }

    public void setRangeStartingMillis(int rangeStartingMillis) {
        this.rangeStartingMillis = rangeStartingMillis;
    }

    public void setRangeEndingHourOfDay(int rangeEndingHourOfDay) {
        this.rangeEndingHourOfDay = rangeEndingHourOfDay;
    }

    public void setRangeEndingMinute(int rangeEndingMinute) {
        this.rangeEndingMinute = rangeEndingMinute;
    }

    public void setRangeEndingSecond(int rangeEndingSecond) {
        this.rangeEndingSecond = rangeEndingSecond;
    }

    public void setRangeEndingMillis(int rangeEndingMillis) {
        this.rangeEndingMillis = rangeEndingMillis;
    }
}
