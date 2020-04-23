package com.github.quartz.impl.redisjobstore.ext;

import com.github.quartz.impl.redisjobstore.constant.FieldConstants;
import org.quartz.JobKey;
import org.quartz.TriggerKey;
import org.quartz.impl.jdbcjobstore.FiredTriggerRecord;

public class FiredTriggerRecordEntity implements FieldConstants {

    private String entryId;

    private long firedTime;

    private long schedTime;

    private String instanceId;

    private String triggerName;

    private String triggerGroup;

    private String state;

    private String jobName;

    private String jobGroup;

    private String priority;

    public String getJobName() {
        return jobName;
    }

    public void setJobName(String jobName) {
        this.jobName = jobName;
    }

    public String getInstanceId() {
        return instanceId;
    }

    public void setInstanceId(String instanceId) {
        this.instanceId = instanceId;
    }

    public long getFiredTime() {
        return firedTime;
    }

    public void setFiredTime(long firedTime) {
        this.firedTime = firedTime;
    }

    public String getTriggerGroup() {
        return triggerGroup;
    }

    public void setTriggerGroup(String triggerGroup) {
        this.triggerGroup = triggerGroup;
    }

    public String getTriggerName() {
        return triggerName;
    }

    public void setTriggerName(String triggerName) {
        this.triggerName = triggerName;
    }

    public long getSchedTime() {
        return schedTime;
    }

    public void setSchedTime(long schedTime) {
        this.schedTime = schedTime;
    }

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }

    public String getJobGroup() {
        return jobGroup;
    }

    public void setJobGroup(String jobGroup) {
        this.jobGroup = jobGroup;
    }

    public String getPriority() {
        return priority;
    }

    public void setPriority(String priority) {
        this.priority = priority;
    }

    public String getEntryId() {
        return entryId;
    }

    public void setEntryId(String entryId) {
        this.entryId = entryId;
    }

    public FiredTriggerRecord trans() {
        FiredTriggerRecord ftr = new FiredTriggerRecord();
        ftr.setFireInstanceId(entryId);
        ftr.setFireTimestamp(firedTime);
        ftr.setScheduleTimestamp(schedTime);
        ftr.setSchedulerInstanceId(instanceId);
        ftr.setTriggerKey(new TriggerKey(triggerName, triggerGroup));
        ftr.setFireInstanceState(state);
        ftr.setJobKey(new JobKey(jobName, jobGroup));
        ftr.setPriority(Integer.parseInt(priority));
        return ftr;
    }
}
