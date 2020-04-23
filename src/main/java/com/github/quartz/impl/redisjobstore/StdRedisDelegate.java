package com.github.quartz.impl.redisjobstore;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.quartz.impl.redisjobstore.constant.FieldConstants;
import com.github.quartz.impl.redisjobstore.constant.RedisConstants;
import com.github.quartz.impl.redisjobstore.delegate.*;
import com.github.quartz.impl.redisjobstore.ext.FiredTriggerRecordEntity;
import com.github.quartz.impl.redisjobstore.mixin.*;
import io.lettuce.core.*;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.sync.*;
import org.quartz.Calendar;
import org.quartz.*;
import org.quartz.impl.JobDetailImpl;
import org.quartz.impl.calendar.BaseCalendar;
import org.quartz.impl.calendar.DailyCalendar;
import org.quartz.impl.jdbcjobstore.FiredTriggerRecord;
import org.quartz.impl.jdbcjobstore.NoSuchDelegateException;
import org.quartz.impl.jdbcjobstore.SchedulerStateRecord;
import org.quartz.impl.jdbcjobstore.TriggerStatus;
import org.quartz.impl.matchers.GroupMatcher;
import org.quartz.impl.triggers.SimpleTriggerImpl;
import org.quartz.spi.ClassLoadHelper;
import org.quartz.spi.OperableTrigger;
import org.slf4j.Logger;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.*;

public class StdRedisDelegate implements RedisConstants, FieldConstants {

    /*
     * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
     *
     * Data members.
     *
     * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
     */

    protected Logger logger = null;

    protected String schedName;

    protected String instanceId;

    protected List<TriggerTypeDelegate> triggerTypeDelegates = new LinkedList<TriggerTypeDelegate>();

    protected AbstractRedisClient redisClient;

    protected StatefulConnection<String, String> statefulConnection;

    protected RedisKeyCommands<String, String> redisKeyCommands;

    protected RedisStringCommands<String, String> redisStringCommands;

    protected RedisScriptingCommands<String, String> redisScriptingCommands;

    protected RedisHashCommands<String, String> redisHashCommands;

    protected RedisSetCommands<String, String> redisSetCommands;

    protected RedisSortedSetCommands<String, String> redisSortedSetCommands;

    protected RedisServerCommands<String, String> redisServerCommands;

    protected ObjectMapper objectMapper;

    /*
     * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
     *
     * Interface.
     *
     * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
     */

    /**
     * @param initString of the format: settingName=settingValue|otherSettingName=otherSettingValue|...
     * @throws NoSuchDelegateException
     */
    public void initialize(Logger logger, String schedName, String instanceId, ClassLoadHelper classLoadHelper, String initString) throws NoSuchDelegateException {

        this.logger = logger;
        this.schedName = schedName;
        this.instanceId = instanceId;

        initJackson(classLoadHelper);
        addDefaultTriggerTypeDelegates();

        if(initString == null)
            return;

        String[] settings = initString.split("\\|");

        for(String setting: settings) {
            String[] parts = setting.split("=");
            String name = parts[0];
            if(parts.length == 1 || parts[1] == null || parts[1].equals(""))
                continue;

            if(name.equals("triggerTypeDelegateClasses")) {

                String[] trigDelegates = parts[1].split(",");

                for(String trigDelClassName: trigDelegates) {
                    try {
                        Class<?> trigDelClass = classLoadHelper.loadClass(trigDelClassName);
                        addTriggerTypeDelegate((TriggerTypeDelegate) trigDelClass.newInstance());
                    } catch (Exception e) {
                        throw new NoSuchDelegateException("Error instantiating TriggerPersistenceDelegate of type: " + trigDelClassName, e);
                    }
                }
            }
            else
                throw new NoSuchDelegateException("Unknown setting: '" + name + "'");
        }
    }

    protected void initJackson(ClassLoadHelper loadHelper) {
        objectMapper = new ObjectMapper()
                .addMixIn(JobDetail.class, JobDetailMixIn.class)
                .addMixIn(CronTrigger.class, CronTriggerMixIn.class)
                .addMixIn(CalendarOffsetTrigger.class, CalendarOffsetTriggerMixIn.class)
                .addMixIn(SimpleTrigger.class, TriggerMixIn.class)
                .addMixIn(DailyTimeIntervalTrigger.class, DailyTimeIntervalTriggerMixIn.class)
                .addMixIn(CalendarIntervalTrigger.class, TriggerMixIn.class)
                .addMixIn(DailyCalendar.class, DailyCalendarMixIn.class)
                .setSerializationInclusion(JsonInclude.Include.NON_NULL)
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        objectMapper.setTypeFactory(objectMapper.getTypeFactory().withClassLoader(loadHelper.getClassLoader()));
    }

    protected void addDefaultTriggerTypeDelegates() {
        addTriggerTypeDelegate(new SimpleTriggerTypeDelegate());
        addTriggerTypeDelegate(new CronTriggerTypeDelegate());
        addTriggerTypeDelegate(new CalendarIntervalTriggerTypeDelegate());
        addTriggerTypeDelegate(new DailyTimeIntervalTriggerTypeDelegate());
        addTriggerTypeDelegate(new CalendarOffsetTriggerTypeDelegate());
    }

    public void addTriggerTypeDelegate(TriggerTypeDelegate delegate) {
        this.triggerTypeDelegates.add(delegate);
    }

    public StdRedisDelegate setRedisClient(AbstractRedisClient redisClient) {
        this.redisClient = redisClient;
        return this;
    }

    public StdRedisDelegate setStatefulConnection(StatefulConnection<String, String> statefulConnection) {
        this.statefulConnection = statefulConnection;
        return this;
    }

    public StdRedisDelegate setRedisKeyCommands(RedisKeyCommands<String, String> redisKeyCommands) {
        this.redisKeyCommands = redisKeyCommands;
        return this;
    }

    public StdRedisDelegate setRedisStringCommands(RedisStringCommands<String, String> redisStringCommands) {
        this.redisStringCommands = redisStringCommands;
        return this;
    }

    public StdRedisDelegate setRedisScriptingCommands(RedisScriptingCommands<String, String> redisScriptingCommands) {
        this.redisScriptingCommands = redisScriptingCommands;
        return this;
    }

    public StdRedisDelegate setRedisHashCommands(RedisHashCommands<String, String> redisHashCommands) {
        this.redisHashCommands = redisHashCommands;
        return this;
    }

    public StdRedisDelegate setRedisSetCommands(RedisSetCommands<String, String> redisSetCommands) {
        this.redisSetCommands = redisSetCommands;
        return this;
    }

    public StdRedisDelegate setRedisSortedSetCommands(RedisSortedSetCommands<String, String> redisSortedSetCommands) {
        this.redisSortedSetCommands = redisSortedSetCommands;
        return this;
    }

    public StdRedisDelegate setRedisServerCommands(RedisServerCommands<String, String> redisServerCommands) {
        this.redisServerCommands = redisServerCommands;
        return this;
    }

    /**
     * lock
     * @param key
     * @param value
     * @param timeout
     * @return
     */
    public boolean obtainLock(String key, String value, long timeout) {
        while (redisStringCommands.set(key, value, new SetArgs().px(timeout).nx()) == null) {
            try {
                Thread.sleep(10L);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        return true;
    }

    /**
     * lua for release lock
     */
    private static final String UNLOCK_LUA =
            "if redis.call(\"get\",KEYS[1]) == ARGV[1] " +
                    "then " +
                    "    return redis.call(\"del\",KEYS[1]) " +
                    "else " +
                    "    return 0 " +
                    "end ";

    /**
     * release lock
     * @param key
     * @param value
     * @return
     */
    public void releaseLock(String key, String value) {
        redisScriptingCommands.eval(UNLOCK_LUA, ScriptOutputType.BOOLEAN, new String[]{key}, value);
    }

    public void shutdown() {
        if (statefulConnection != null) {
            statefulConnection.close();
        }
        if (redisClient != null) {
            redisClient.shutdown();
        }
    }

    // key start
    protected boolean exists(String... key) {
        return redisKeyCommands.exists(key) > 0;
    }

    protected int del(String... keys) {
        return redisKeyCommands.del(keys).intValue();
    }
    // key end

    // string start
    protected String set(String key, String value) {
        return redisStringCommands.set(key, value);
    }

    protected String get(String key) {
        return redisStringCommands.get(key);
    }
    // string end

    // set start
    protected long sadd(String key, String... members) {
       return redisSetCommands.sadd(key, members);
    }

    protected boolean sismember(String key, String member) {
        return redisSetCommands.sismember(key, member);
    }

    protected Set<String> smembers(String key) {
        return redisSetCommands.smembers(key);
    }

    protected int scard(String key) {
        return redisSetCommands.scard(key).intValue();
    }

    protected int srem(String key, String... members) {
        return redisSetCommands.srem(key, members).intValue();
    }

    protected List<String> sscan(String key, String match) {
        return redisSetCommands.sscan(key, new ScanArgs().match(match)).getValues();
    }
    /*
    protected boolean sscan(String key, String match, int limit) {
        return redisSetCommands.sscan(key, new ScanArgs().match(match).limit(limit)).getValues().size() > 0;
    }*/
    /**
     * make set empty
     * @param key
     * @return
     */
    /*protected Set<String> spop(String key) {
        return redisSetCommands.spop(key, 0);
    }*/
    // set end

    // sorted set start
    protected long zadd(String key, double score, String member) {
        return redisSortedSetCommands.zadd(key, score, member);
    }

    protected List<String> zrangebyscore(String key, long includedEnd) {
        return redisSortedSetCommands.zrangebyscore(key, Range.create(0, includedEnd));
    }

    protected List<String> zrangebyscoreExcluded(String key, long excludedEnd) {
        return redisSortedSetCommands.zrangebyscore(key,
                Range.from(Range.Boundary.excluding(0), Range.Boundary.excluding(excludedEnd)));
    }

    /*
    protected int zcard(String key) {
        return redisSortedSetCommands.zcard(key).intValue();
    }
    */

    protected int zrem(String key, String... members) {
        return redisSortedSetCommands.zrem(key, members).intValue();
    }

    /*
    protected List<ScoredValue<String>> zscan(String key, String match) {
        return redisSortedSetCommands.zscan(key, new ScanArgs().match(match)).getValues();
    }
    */
    // sorted set end

    // hash(obj) start
    protected String hmset(String key, Map<String, String> map) {
        return redisHashCommands.hmset(key, map);
    }

    protected boolean hset(String key, String field, String value) {
        return redisHashCommands.hset(key, field, value);
    }

    protected Map<String, String> hgetall(String key) {
        return redisHashCommands.hgetall(key);
    }

    protected String hget(String key, String field) {
        return redisHashCommands.hget(key, field);
    }

    protected List<KeyValue<String, String>> hmget(String key, String... fields) {
        return redisHashCommands.hmget(key, fields);
    }

    /*
    protected int hdel(String key, String... fields) {
        return redisHashCommands.hdel(key, fields).intValue();
    }
    */
    // hash(obj) end

    // server start
    protected String flushdb() {
        return redisServerCommands.flushdb();
    }
    // server end

    protected String keyOfJob(JobKey jobKey) {
        return MessageFormat.format(KEY_JOB, schedName, jobKey.getName(), jobKey.getGroup());
    }

    /*
    protected String keyOfJob(String jobName, String groupName) {
        return MessageFormat.format(KEY_JOB, schedName, jobName, groupName);
    }
    */

    protected String keyOfJobs() {
        return MessageFormat.format(KEY_JOBS, schedName);
    }

    protected String keyOfTrigger(TriggerKey triggerKey) {
        return MessageFormat.format(KEY_TRIGGER, schedName, triggerKey.getName(), triggerKey.getGroup());
    }

    protected String keyOfTrigger(String triggerName, String groupName) {
        return MessageFormat.format(KEY_TRIGGER, schedName, triggerName, groupName);
    }

    protected String keyOfTriggers() {
        return MessageFormat.format(KEY_TRIGGERS, schedName);
    }

    protected String keyOfWaitingStateTriggers() {
        return MessageFormat.format(KEY_WAITING_STATE_TRIGGERS, schedName);
    }

    protected String keyOfOtherStateTriggers(String state) {
        return MessageFormat.format(KEY_OTHER_STATE_TRIGGERS, state, schedName);
    }

    protected String keyOfJobTriggers(JobKey jobKey) {
        return MessageFormat.format(KEY_JOB_TRIGGERS, schedName, jobKey.getName(), jobKey.getGroup());
    }

    protected String keyOfFiredInstances() {
        return MessageFormat.format(KEY_FIRED_INSTANCES, schedName);
    }

    protected String keyOfFiredJobs(String instanceName) {
        return MessageFormat.format(KEY_FIRED_JOBS, schedName, instanceName);
    }

    protected String keyOfFiredJob(String entryId) {
        return MessageFormat.format(KEY_FIRED_JOB, schedName, entryId);
    }

    protected String keyOfPausedTriggerGroups() {
        return MessageFormat.format(KEY_PAUSED_TRIGGER_GROUPS, schedName);
    }

    protected String keyOfCalendar(String calendarName) {
        return MessageFormat.format(KEY_CALENDAR, schedName, calendarName);
    }

    protected String keyOfCalendars() {
        return MessageFormat.format(KEY_CALENDARS, schedName);
    }

    protected String keyOfCalendarTriggers(String calendarName) {
        return MessageFormat.format(KEY_CALENDAR_TRIGGERS, schedName, calendarName);
    }

    protected String keyOfSchedulerStates() {
        return MessageFormat.format(KEY_SCHEDULER_STATES, schedName);
    }

    protected String keyOfSchedulerState(String instanceId) {
        return MessageFormat.format(KEY_SCHEDULER_STATE, schedName, instanceId);
    }

    protected String[] splitValue(String originValue) {
        return originValue.split(VALUE_DELIMITER);
    }

    protected String joinValue(TriggerKey triggerKey) {
        return joinValue(triggerKey.getName(), triggerKey.getGroup());
    }

    protected String joinValue(JobKey jobKey) {
        return joinValue(jobKey.getName(), jobKey.getGroup());
    }

    protected String joinValue(String elementName, String groupName) {
        return new StringBuilder(elementName).append(VALUE_DELIMITER).append(groupName).toString();
    }

    protected TriggerTypeDelegate findTriggerTypeDelegate(OperableTrigger trigger)  {
        for(TriggerTypeDelegate delegate: triggerTypeDelegates) {
            if(delegate.canHandleTriggerType(trigger))
                return delegate;
        }

        return null;
    }

    public TriggerTypeDelegate findTriggerTypeDelegate(String discriminator)  {
        for(TriggerTypeDelegate delegate: triggerTypeDelegates) {
            if(delegate.getHandledTriggerTypeDiscriminator().equals(discriminator))
                return delegate;
        }

        return null;
    }

    //---------------------------------------------------------------------------
    // startup / recovery
    //---------------------------------------------------------------------------

    /**
     * <p>
     * Insert the job detail record.
     * </p>
     *
     * @param newState
     *          the new state for the triggers
     * @param oldState1
     *          the first old state to update
     * @param oldState2
     *          the second old state to update
     * @return number of rows updated
     */
    public int updateTriggerStatesFromOtherStates(String newState, String oldState1, String oldState2) {
        Set<String> groupWithNames1 = smembers(keyOfOtherStateTriggers(oldState1));
        Set<String> groupWithNames2 = smembers(keyOfOtherStateTriggers(oldState2));
        if (groupWithNames1.size() == 0 && groupWithNames2.size() == 0) {
            return 0;
        }
        String oldState1Key = keyOfOtherStateTriggers(oldState1);
        String oldState2Key = keyOfOtherStateTriggers(oldState2);
        if (groupWithNames1.size() > 0) {
            srem(oldState1Key, groupWithNames1.toArray(new String[]{}));
        }
        if (groupWithNames2.size() > 0) {
            srem(oldState2Key, groupWithNames2.toArray(new String[]{}));
        }
        if (STATE_WAITING.equals(newState)) {
            Set<OperableTrigger> set = new HashSet<>(groupWithNames1.size() + groupWithNames2.size());
            for (String groupWithName : groupWithNames1) {
                String[] array = splitValue(groupWithName);
                set.add(selectTrigger(new TriggerKey(array[0], array[1])));
            }
            for (String groupWithName : groupWithNames2) {
                String[] array = splitValue(groupWithName);
                set.add(selectTrigger(new TriggerKey(array[0], array[1])));
            }
            String waitingStateKey = keyOfWaitingStateTriggers();
            for (OperableTrigger operableTrigger : set) {
                if (operableTrigger.getNextFireTime() != null) {
                    zadd(waitingStateKey,
                            operableTrigger.getNextFireTime().getTime(),
                            joinValue(operableTrigger.getKey()));
                    hset(keyOfTrigger(operableTrigger.getKey()), FIELD_STATE, newState);
                }
            }
        } else {
            Set<String> set = new HashSet<>(groupWithNames1.size() + groupWithNames2.size());
            set.addAll(groupWithNames1);
            set.addAll(groupWithNames2);
            String[] allGroupWithNames = set.toArray(new String[]{});
            if (allGroupWithNames.length > 0) {
                sadd(keyOfOtherStateTriggers(newState), allGroupWithNames);
                for (String groupWithName : allGroupWithNames) {
                    String[] array = splitValue(groupWithName);
                    hset(keyOfTrigger(array[0], array[1]), FIELD_STATE, newState);
                }
            }
        }
        return groupWithNames1.size() + groupWithNames2.size();
    }

    /**
     * <p>
     * Select all of the triggers in a given state.
     * </p>
     *
     * @param state
     *          the state the triggers must be in
     * @return an array of trigger <code>Key</code> s
     */
    public List<TriggerKey> selectTriggersInState(String state) {
        LinkedList<TriggerKey> list = new LinkedList<TriggerKey>();

        Set<String> groupWithNames = smembers(keyOfOtherStateTriggers(state));
        for (String groupWithName : groupWithNames) {
            String[] array = splitValue(groupWithName);
            list.add(new TriggerKey(array[0], array[1]));
        }
        return list;
    }

    /**
     * <p>
     * Get the names of all of the triggers in the given state that have
     * misfired - according to the given timestamp.  No more than count will
     * be returned.
     * </p>
     *
     * @param count The most misfired triggers to return, negative for all
     * @param resultList Output parameter.  A List of
     *      <code>{@link org.quartz.utils.Key}</code> objects.  Must not be null.
     *
     * @return Whether there are more misfired triggers left to find beyond
     *         the given count.
     */
    public boolean hasMisfiredTriggersInState(String state1,
                                              long ts, int count, List<TriggerKey> resultList) {
        List<String> groupWithTriggerNames = zrangebyscoreExcluded(keyOfWaitingStateTriggers(), ts);

        List<Map<String, String>> list = new ArrayList<>(groupWithTriggerNames.size());
        for (int i = 0; i < groupWithTriggerNames.size(); i++) {
            String[] array = splitValue(groupWithTriggerNames.get(i));
            List<KeyValue<String, String>> keyValues = hmget(keyOfTrigger(array[0], array[1]), FIELD_MISFIRE_INSTRUCTION, FIELD_STATE, FIELD_NEXT_FIRE_TIME, FIELD_PRIORITY);
            if (!"-1".equals(keyValues.get(0).getValue()) && state1.equals(keyValues.get(1).getValue())) {
                Map<String, String> map = new HashMap<>(6);
                for (KeyValue<String, String> keyValue : keyValues) {
                    map.put(keyValue.getKey(), keyValue.getValue());
                }
                map.put(FIELD_TRIGGER_NAME, array[0]);
                map.put(FIELD_TRIGGER_GROUP, array[1]);
                list.add(map);
            }
        }
        list.sort(Comparator.comparing((Map<String, String> o) -> o.get(FIELD_NEXT_FIRE_TIME))
                .thenComparing(o -> o.get(FIELD_PRIORITY)).reversed());

        Iterator<Map<String, String>> it = list.iterator();

        boolean hasReachedLimit = false;
        while (it.hasNext() && !hasReachedLimit) {
            if (resultList.size() == count) {
                hasReachedLimit = true;
            } else {
                Map<String, String> map = it.next();
                String triggerName = map.get(FIELD_TRIGGER_NAME);
                String groupName = map.get(FIELD_TRIGGER_GROUP);
                resultList.add(new TriggerKey(triggerName, groupName));
            }
        }

        return hasReachedLimit;
    }

    /**
     * <p>
     * Get the number of triggers in the given states that have
     * misfired - according to the given timestamp.
     * </p>
     *
     */
    public int countMisfiredTriggersInState(String state1, long ts) {
        int count = 0;
        List<String> groupWithTriggerNames = zrangebyscoreExcluded(keyOfWaitingStateTriggers(), ts);

        for (int i = 0; i < groupWithTriggerNames.size(); i++) {
            String[] array = splitValue(groupWithTriggerNames.get(i));
            List<KeyValue<String, String>> keyValues = hmget(keyOfTrigger(array[0], array[1]), FIELD_MISFIRE_INSTRUCTION, FIELD_STATE);
            if (!"-1".equals(keyValues.get(0).getValue()) && state1.equals(keyValues.get(1).getValue())) {
                count++;
            }
        }
        return count;
    }

    /**
     * <p>
     * Select all of the triggers for jobs that are requesting recovery. The
     * returned trigger objects will have unique "recoverXXX" trigger names and
     * will be in the <code>{@link
     * org.quartz.Scheduler}.DEFAULT_RECOVERY_GROUP</code>
     * trigger group.
     * </p>
     *
     * <p>
     * In order to preserve the ordering of the triggers, the fire time will be
     * set from the <code>COL_FIRED_TIME</code> column in the <code>TABLE_FIRED_TRIGGERS</code>
     * table. The caller is responsible for calling <code>computeFirstFireTime</code>
     * on each returned trigger. It is also up to the caller to insert the
     * returned triggers to ensure that they are fired.
     * </p>
     *
     * @return an array of <code>{@link org.quartz.Trigger}</code> objects
     */
    public List<OperableTrigger> selectTriggersForRecoveringJobs() {
        long dumId = System.currentTimeMillis();
        LinkedList<OperableTrigger> list = new LinkedList<OperableTrigger>();

        Set<String> entryIds = smembers(keyOfFiredJobs(instanceId));
        for (String entryId : entryIds) {
            Map<String, String> firedJobMap = hgetall(keyOfFiredJob(entryId));
            String jobName = firedJobMap.get(FIELD_JOB_NAME);
            String groupName = firedJobMap.get(FIELD_JOB_GROUP);
            String priority = firedJobMap.get(FIELD_PRIORITY);
            String requestsRecover = firedJobMap.get(FIELD_REQUESTS_RECOVERY);
            if (Boolean.parseBoolean(requestsRecover)) {
                long firedTime = Long.parseLong(firedJobMap.get(FIELD_FIRED_TIME));
                long scheduledTime = Long.parseLong(firedJobMap.get(FIELD_SCHED_TIME));
                SimpleTriggerImpl rcvryTrig = (SimpleTriggerImpl) TriggerBuilder.newTrigger()
                        .withIdentity("recover_"  + instanceId + "_" + dumId++,
                        Scheduler.DEFAULT_RECOVERY_GROUP).startAt(new Date(scheduledTime)).build();
                rcvryTrig.setJobName(jobName);
                rcvryTrig.setJobGroup(groupName);
                rcvryTrig.setPriority(Integer.parseInt(priority));
                rcvryTrig.setMisfireInstruction(SimpleTrigger.MISFIRE_INSTRUCTION_IGNORE_MISFIRE_POLICY);

                String triggerName = firedJobMap.get(FIELD_TRIGGER_NAME);
                JobDataMap jd = selectTriggerJobDataMap(triggerName, groupName);
                jd.put(Scheduler.FAILED_JOB_ORIGINAL_TRIGGER_NAME, triggerName);
                jd.put(Scheduler.FAILED_JOB_ORIGINAL_TRIGGER_GROUP, groupName);
                jd.put(Scheduler.FAILED_JOB_ORIGINAL_TRIGGER_FIRETIME_IN_MILLISECONDS, String.valueOf(firedTime));
                jd.put(Scheduler.FAILED_JOB_ORIGINAL_TRIGGER_SCHEDULED_FIRETIME_IN_MILLISECONDS, String.valueOf(scheduledTime));
                rcvryTrig.setJobDataMap(jd);

                list.add(rcvryTrig);
            }
        }
        return list;
    }

    /**
     * <p>
     * Delete all fired triggers.
     * </p>
     *
     * @return the number of rows deleted
     */
    public int deleteFiredTriggers() {
        String keyOfFiredInstances = keyOfFiredInstances();
        Set<String> instanceNames = smembers(keyOfFiredInstances);
        for (String instanceName : instanceNames) {
            String keyOfFiredJobs = keyOfFiredJobs(instanceName);
            Set<String> entryIds = smembers(keyOfFiredJobs);
            for (String entryId : entryIds) {
                del(keyOfFiredJob(entryId));
            }
            del(keyOfFiredJobs);
        }

        return del(keyOfFiredInstances);
    }

    public int deleteFiredTriggers(String theInstanceId) {
        String keyOfFiredJobs = keyOfFiredJobs(theInstanceId);
        Set<String> entryIds = smembers(keyOfFiredJobs);
        for (String entryId : entryIds) {
            del(keyOfFiredJob(entryId));
        }
        del(keyOfFiredJobs);
        return srem(keyOfFiredInstances(), theInstanceId);
    }

    /**
     * Clear (delete!) all scheduling data - all {@link Job}s, {@link Trigger}s
     * {@link Calendar}s.
     *
     */
    public void clearData() {
        flushdb();
    }

    //---------------------------------------------------------------------------
    // jobs
    //---------------------------------------------------------------------------

    /**
     * <p>
     * Insert the job detail record.
     * </p>
     *
     * @param job
     *          the job to insert
     * @return number of rows inserted
     */
    public void insertJobDetail(JobDetail job) {
        sadd(keyOfJobs(), joinValue(job.getKey()));
        updateJobDetail(job);
    }

    /**
     * <p>
     * Update the job detail record.
     * </p>
     *
     * @param job
     *          the job to update
     */
    public void updateJobDetail(JobDetail job) {
        String keyOfJobDetail = keyOfJob(job.getKey());
        Map<String, String> map = objectMapper
                .convertValue(job, new TypeReference<HashMap<String, String>>() {});
        map.put(FieldConstants.FIELD_REQUESTS_RECOVERY, String.valueOf(job.requestsRecovery()));
        hmset(keyOfJobDetail, map);
    }

    /**
     * <p>
     * Get the names of all of the triggers associated with the given job.
     * </p>
     *
     * @return an array of <code>{@link
     * org.quartz.utils.Key}</code> objects
     */
    public List<TriggerKey> selectTriggerKeysForJob(JobKey jobKey) {
        LinkedList<TriggerKey> list = new LinkedList<TriggerKey>();

        Set<String> groupWithNames = smembers(keyOfJobTriggers(jobKey));
        for (String item : groupWithNames) {
            String[] array = splitValue(item);
            list.add(new TriggerKey(array[0], array[1]));
        }
        return list;
    }

    /**
     * <p>
     * Delete the job detail record for the given job.
     * </p>
     *
     * @return the number of rows deleted
     */
    public long deleteJobDetail(JobKey jobKey) {
        String kjt = keyOfJobTriggers(jobKey);
        if (exists(kjt)) {
            srem(kjt);
        }
        del(keyOfJob(jobKey));
        return srem(keyOfJobs(), joinValue(jobKey));
    }

    /**
     * <p>
     * Check whether or not the given job exists.
     * </p>
     *
     * @return true if the job exists, false otherwise
     */
    protected boolean jobExists(JobKey jobKey) {
        String key = keyOfJob(jobKey);
        return exists(key);
    }

    /**
     * <p>
     * Update the job data map for the given job.
     * </p>
     *
     * @param job
     *          the job to update
     */
    public void updateJobData(JobDetail job) {
        JobDataMap jobDataMap = job.getJobDataMap();
        String s = objectMapper.convertValue(jobDataMap, String.class);
        hset(keyOfJob(job.getKey()), FIELD_JOB_DATA_MAP, s);
    }

    /**
     * <p>
     * Select the JobDetail object for a given job name / group name.
     * </p>
     *
     * @return the populated JobDetail object
     *           the job class could not be found
     */
    public JobDetail selectJobDetail(JobKey jobKey) {
        if (!jobExists(jobKey)) {
            return null;
        }
        Map<String, String> jobDetailMap = hgetall(keyOfJob(jobKey));
        return objectMapper.convertValue(jobDetailMap, JobDetailImpl.class);
    }

    /**
     * <p>
     * Select the total number of jobs stored.
     * </p>
     *
     * @return the total number of jobs stored
     */
    public int selectNumJobs() {
        return scard(keyOfJobs());
    }

    /**
     * <p>
     * Select all of the job group names that are stored.
     * </p>
     *
     * @return an array of <code>String</code> group names
     */
    public List<String> selectJobGroups() {
        Set<String> groupWithNames = smembers(keyOfJobs());
        Set<String> groups = new HashSet<>();
        for (String item : groupWithNames) {
            String[] array = splitValue(item);
            groups.add(array[1]);
        }
        return new LinkedList<>(groups);
    }

    /**
     * <p>
     * Select all of the jobs contained in a given group.
     * </p>
     *
     * @param matcher
     *          the groupMatcher to evaluate the jobs against
     * @return an array of <code>String</code> job names
     */
    public Set<JobKey> selectJobsInGroup(GroupMatcher<JobKey> matcher) {
        List<String> groupWithNames = sscan(keyOfJobs(), toGroupLikeClause(matcher));
        Set<JobKey> results = new HashSet<JobKey>(groupWithNames.size());
        for (String item : groupWithNames) {
            String[] array = splitValue(item);
            results.add(new JobKey(array[0], array[1]));
        }
        return results;
    }

    protected String toGroupLikeClause(final GroupMatcher<?> matcher) {
        String groupName;
        switch(matcher.getCompareWithOperator()) {
            case EQUALS:
                groupName = "*" + VALUE_DELIMITER + matcher.getCompareToValue();
                break;
            case CONTAINS:
                groupName = "*" + VALUE_DELIMITER + "*" + matcher.getCompareToValue() + "*";
                break;
            case ENDS_WITH:
                groupName = "*" + VALUE_DELIMITER + "*" + matcher.getCompareToValue();
                break;
            case STARTS_WITH:
                groupName = "*" + VALUE_DELIMITER + matcher.getCompareToValue() + "*";
                break;
            case ANYTHING:
                groupName = "*";
                break;
            default:
                throw new UnsupportedOperationException("Don't know how to translate " + matcher.getCompareWithOperator() + " into SQL");
        }
        return groupName;
    }

    //---------------------------------------------------------------------------
    // triggers
    //---------------------------------------------------------------------------

    /**
     * <p>
     * Insert the base trigger data.
     * </p>
     *
     * @param trigger
     *          the trigger to insert
     * @param state
     *          the state that the trigger should be stored in
     * @return the number of rows inserted
     */
    public void insertTrigger(OperableTrigger trigger, String state, JobDetail job) {
        updateTrigger(trigger, state, job);
        sadd(keyOfTriggers(), joinValue(trigger.getKey()));
    }

    /**
     * <p>
     * Insert the blob trigger data.
     * </p>
     *
     * @param trigger
     *          the trigger to insert
     */
    public void updateTrigger(OperableTrigger trigger, String state, JobDetail job) {
        Map<String, String> triggerMap = objectMapper
                .convertValue(trigger, new TypeReference<HashMap<String, String>>() {});
        TriggerTypeDelegate tDel = findTriggerTypeDelegate(trigger);
        String type;
        if(tDel != null) {
            type = tDel.getHandledTriggerTypeDiscriminator();
        } else {
            type = TTYPE_BLOB;
        }
        triggerMap.put(FIELD_STATE, state);
        triggerMap.put(FIELD_TYPE, type);
        long nextFireTime = -1;
        if (trigger.getNextFireTime() != null) {
            nextFireTime = trigger.getNextFireTime().getTime();
        }
        triggerMap.put(FIELD_NEXT_FIRE_TIME, String.valueOf(nextFireTime));
        long prevFireTime = -1;
        if (trigger.getPreviousFireTime() != null) {
            prevFireTime = trigger.getPreviousFireTime().getTime();
        }
        triggerMap.put(FIELD_PREV_FIRE_TIME, String.valueOf(prevFireTime));
        String oldState = hget(keyOfTrigger(trigger.getKey()), FIELD_STATE);
        hmset(keyOfTrigger(trigger.getKey()), triggerMap); // add trigger
        sadd(keyOfJobTriggers(job.getKey()), joinValue(trigger.getKey())); // add job trigger

        String joinValue = joinValue(trigger.getKey());
        if (STATE_WAITING.equals(oldState)) {
            zrem(keyOfWaitingStateTriggers(), joinValue); // remove waiting state of trigger
        } else {
            srem(keyOfOtherStateTriggers(oldState), joinValue); // remove old state of trigger
        }
        if (STATE_WAITING.equals(state)) {
            if (trigger.getNextFireTime() != null) {
                zadd(keyOfWaitingStateTriggers(),
                        trigger.getNextFireTime().getTime(),
                        joinValue(trigger.getKey()));
            }
        } else {
            sadd(keyOfOtherStateTriggers(state), joinValue);
        }
    }

    /**
     * <p>
     * Check whether or not a trigger exists.
     * </p>
     *
     * @return true if the trigger exists, false otherwise
     */
    protected boolean triggerExists(TriggerKey triggerKey) {
        String key = keyOfTrigger(triggerKey);
        return exists(key);
    }

    /**
     * <p>
     * Update the state for a given trigger.
     * </p>
     *
     * @param state
     *          the new state for the trigger
     * @return the number of rows updated
     */
    public void updateTriggerState(TriggerKey triggerKey,
                                  String state) {
        String key = keyOfTrigger(triggerKey);
        List<KeyValue<String, String>> keyValues = hmget(key, FIELD_STATE, FIELD_NEXT_FIRE_TIME);
        if (keyValues.size() > 0) {
            hset(key, FIELD_STATE, state);

            String oldState = keyValues.get(0).getValue();
            String groupWithName = joinValue(triggerKey);
            // remove old state from states
            if (STATE_WAITING.equals(oldState)) {
                zrem(keyOfWaitingStateTriggers(), groupWithName);
            } else {
                srem(keyOfOtherStateTriggers(oldState), groupWithName);
            }
            // add new state to states
            if (STATE_WAITING.equals(state)) {
                String theNextFireTime = keyValues.get(1).getValue();
                if (theNextFireTime != null && !"-1".equals(theNextFireTime)) {
                    zadd(keyOfWaitingStateTriggers(), Long.parseLong(theNextFireTime), groupWithName);
                }
            } else {
                sadd(keyOfOtherStateTriggers(state), groupWithName);
            }
        }
    }

    /**
     * <p>
     * Update all triggers in the given group to the given new state, if they
     * are in one of the given old states.
     * </p>
     *
     * @param matcher
     *          the groupMatcher to evaluate the triggers against
     * @param newState
     *          the new state for the trigger
     * @param oldState1
     *          one of the old state the trigger must be in
     * @param oldState2
     *          one of the old state the trigger must be in
     * @return int the number of rows updated
     */
    public void updateTriggerGroupStateFromOtherStates(GroupMatcher<TriggerKey> matcher, String newState,
                                                       String oldState1, String oldState2) {
        Set<TriggerKey> triggerKeys = selectTriggersInGroup(matcher);
        for (TriggerKey triggerKey : triggerKeys) {
            String keyOfTrigger = keyOfTrigger(triggerKey);
            List<KeyValue<String, String>> keyValues = hmget(keyOfTrigger, FIELD_STATE, FIELD_NEXT_FIRE_TIME);
            if (keyValues.size() > 0) {
                String theState = keyValues.get(0).getValue();
                if (theState != null && (theState.equals(oldState1) || theState.equals(oldState2))) {
                    hset(keyOfTrigger, FIELD_STATE, newState); // change trigger state from old to new

                    String groupWithName = joinValue(triggerKey);
                    // remove old state1 from states
                    if (STATE_WAITING.equals(oldState1)) {
                        zrem(keyOfWaitingStateTriggers(), groupWithName);
                    } else {
                        srem(keyOfOtherStateTriggers(oldState1), groupWithName);
                    }
                    // remove old state2 from states
                    if (STATE_WAITING.equals(oldState2)) {
                        zrem(keyOfWaitingStateTriggers(), groupWithName);
                    } else {
                        srem(keyOfOtherStateTriggers(oldState2), groupWithName);
                    }
                    // add new state to states
                    if (STATE_WAITING.equals(newState)) {
                        String theNextFireTime = keyValues.get(1).getValue();
                        if (theNextFireTime != null && !"-1".equals(theNextFireTime)) {
                            zadd(keyOfWaitingStateTriggers(), Long.parseLong(theNextFireTime), groupWithName);
                        }
                    } else {
                        sadd(keyOfOtherStateTriggers(newState), groupWithName);
                    }

                }
            }
        }
    }

    /**
     * <p>
     * Update the given trigger to the given new state, if it is in the given
     * old state.
     * </p>
     *
     * @param newState
     *          the new state for the trigger
     * @param oldState
     *          the old state the trigger must be in
     * @return int the number of rows updated
     */
    public int updateTriggerStateFromOtherState(TriggerKey triggerKey, String newState, String oldState) {
        String keyOfTrigger = keyOfTrigger(triggerKey);
        List<KeyValue<String, String>> keyValues = hmget(keyOfTrigger, FIELD_STATE, FIELD_NEXT_FIRE_TIME);
        if (keyValues.size() > 0) {
            String theState = keyValues.get(0).getValue();
            if (theState != null && theState.equals(oldState)) {
                hset(keyOfTrigger, FIELD_STATE, newState); // change trigger state from old to new

                String groupWithName = joinValue(triggerKey);
                // remove old state from states
                if (STATE_WAITING.equals(oldState)) {
                    zrem(keyOfWaitingStateTriggers(), groupWithName);
                } else {
                    srem(keyOfOtherStateTriggers(oldState), groupWithName);
                }
                // add new state to states
                if (STATE_WAITING.equals(newState)) {
                    String theNextFireTime = keyValues.get(1).getValue();
                    if (theNextFireTime != null && !"-1".equals(theNextFireTime)) {
                        zadd(keyOfWaitingStateTriggers(), Long.parseLong(theNextFireTime), groupWithName);
                    }
                } else {
                    sadd(keyOfOtherStateTriggers(newState), groupWithName);
                }

                return 1;
            }
        }
        return 0;
    }

    /**
     * <p>
     * Update all of the triggers of the given group to the given new state, if
     * they are in the given old state.
     * </p>
     *
     * @param matcher
     *          the groupMatcher to evaluate the triggers against
     * @param newState
     *          the new state for the trigger group
     * @param oldState
     *          the old state the triggers must be in
     * @return int the number of rows updated
     */
    public void updateTriggerGroupStateFromOtherState(GroupMatcher<TriggerKey> matcher, String newState, String oldState) {
        Set<TriggerKey> triggerKeys = selectTriggersInGroup(matcher);
        for (TriggerKey triggerKey : triggerKeys) {
            updateTriggerStateFromOtherState(triggerKey, newState, oldState);
        }
    }

    /**
     * <p>
     * Update the states of all triggers associated with the given job.
     * </p>
     *
     * @param state
     *          the new state for the triggers
     * @return the number of rows updated
     */
    public void updateTriggerStatesForJob(JobKey jobKey,
                                         String state) {
        Set<String> groupWithNames = smembers(keyOfJobTriggers(jobKey));
        for (String item : groupWithNames) {
            String[] array = splitValue(item);
            updateTriggerState(new TriggerKey(array[0], array[1]), state);
        }
    }

    public void updateTriggerStatesForJobFromOtherState(JobKey jobKey, String state, String oldState) {
        Set<String> groupWithNames = smembers(keyOfJobTriggers(jobKey));
        for (String item : groupWithNames) {
            String[] array = splitValue(item);
            updateTriggerStateFromOtherState(new TriggerKey(array[0], array[1]), state, oldState);
        }
    }

    /**
     * <p>
     * Delete the base trigger data for a trigger.
     * </p>
     *
     * @return the number of rows deleted
     */
    public long deleteTrigger(TriggerKey triggerKey) {
        String value = joinValue(triggerKey);
        List<KeyValue<String, String>> keyValues = hmget(keyOfTrigger(triggerKey), FIELD_JOB_NAME, FIELD_JOB_GROUP, FIELD_STATE);
        srem(keyOfJobTriggers(new JobKey(keyValues.get(0).getValue(), keyValues.get(1).getValue())), value);  // remove trigger in job-triggers
        del(keyOfTrigger(triggerKey)); // remove trigger
        String state = keyValues.get(2).getValue();
        if (STATE_WAITING.equals(state)) {
            zrem(keyOfWaitingStateTriggers(), value); // remove waiting state triggers
        } else {
            srem(keyOfOtherStateTriggers(state), value); // remove other state triggers
        }
        return srem(keyOfTriggers(), value); // remove trigger in triggers
    }

    /**
     * <p>
     * Select the number of triggers associated with a given job.
     * </p>
     *
     * @return the number of triggers for the given job
     */
    public int selectNumTriggersForJob(JobKey jobKey) {
        return scard(keyOfJobTriggers(jobKey));
    }

    /**
     * <p>
     * Select the job to which the trigger is associated.
     * </p>
     *
     * @return the <code>{@link org.quartz.JobDetail}</code> object
     *         associated with the given trigger
     */
    public JobDetail selectJobForTrigger(TriggerKey triggerKey) {
        String jobName = hget(keyOfTrigger(triggerKey), FIELD_JOB_NAME);
        Map<String, String> jobDetailMap = hgetall(keyOfJob(new JobKey(jobName, triggerKey.getGroup())));
        JobDetailImpl job = objectMapper.convertValue(jobDetailMap, JobDetailImpl.class);
        return job;
    }

    /**
     * <p>
     * Select the triggers for a job
     * </p>
     *
     * @return an array of <code>(@link org.quartz.Trigger)</code> objects
     *         associated with a given job.
     */
    public List<OperableTrigger> selectTriggersForJob(JobKey jobKey) {

        LinkedList<OperableTrigger> trigList = new LinkedList<OperableTrigger>();

        Set<String> groupWithNames = smembers(keyOfJobTriggers(jobKey));
        for (String item : groupWithNames) {
            String[] array = splitValue(item);
            OperableTrigger t = selectTrigger(new TriggerKey(array[0], array[1]));
            if(t != null) {
                trigList.add(t);
            }
        }

        return trigList;
    }

    public List<OperableTrigger> selectTriggersForCalendar(String calName) {

        LinkedList<OperableTrigger> trigList = new LinkedList<OperableTrigger>();
        Set<String> triggerGroupWithNames = smembers(keyOfCalendarTriggers(calName));
        for (String groupWithName : triggerGroupWithNames) {
            String[] array = splitValue(groupWithName);
            trigList.add(selectTrigger(new TriggerKey(array[0], array[1])));
        }
        return trigList;
    }

    /**
     * <p>
     * Select a trigger.
     * </p>
     *
     * @return the <code>{@link org.quartz.Trigger}</code> object
     * @throws JobPersistenceException
     */
    public OperableTrigger selectTrigger(TriggerKey triggerKey) {
        Map<String, String> triggerMap = hgetall(keyOfTrigger(triggerKey));
        String type = triggerMap.get(FIELD_TYPE);
        TriggerTypeDelegate tDel = findTriggerTypeDelegate(type);
        Class<? extends OperableTrigger> triggerClass = tDel.getTriggerClass();
        OperableTrigger operableTrigger = objectMapper.convertValue(triggerMap, triggerClass);
        return operableTrigger;
    }

    /**
     * <p>
     * Select a trigger's JobDataMap.
     * </p>
     *
     * @param triggerName
     *          the name of the trigger
     * @param groupName
     *          the group containing the trigger
     * @return the <code>{@link org.quartz.JobDataMap}</code> of the Trigger,
     * never null, but possibly empty.
     */
    public JobDataMap selectTriggerJobDataMap(String triggerName,
                                              String groupName) {
        String jobDataMapString = hget(keyOfTrigger(triggerName, groupName), FIELD_JOB_DATA_MAP);
        if (jobDataMapString != null) {
            return objectMapper.convertValue(jobDataMapString, JobDataMap.class);
        }
        return new JobDataMap();
    }

    /**
     * <p>
     * Select a trigger' state value.
     * </p>
     *
     * @return the <code>{@link org.quartz.Trigger}</code> object
     */
    public String selectTriggerState(TriggerKey triggerKey) {
        String state = hget(keyOfTrigger(triggerKey), FIELD_STATE);
        return (state != null ? state : STATE_DELETED).intern();
    }

    /**
     * <p>
     * Select a trigger' status (state & next fire time).
     * </p>
     *
     * @return a <code>TriggerStatus</code> object, or null
     */
    public TriggerStatus selectTriggerStatus(TriggerKey triggerKey) {
        Map<String, String> triggerMap = hgetall(keyOfTrigger(triggerKey));
        String state = triggerMap.get(FIELD_STATE);
        long nextFireTime = Long.parseLong(triggerMap.get(FIELD_NEXT_FIRE_TIME));
        String jobName = triggerMap.get(FIELD_JOB_NAME);
        String jobGroup = triggerMap.get(FIELD_JOB_GROUP);

        Date nft = null;
        if (nextFireTime > 0) {
            nft = new Date(nextFireTime);
        }

        TriggerStatus status = new TriggerStatus(state, nft);
        status.setKey(triggerKey);
        status.setJobKey(new JobKey(jobName, jobGroup));

        return status;

    }

    /**
     * <p>
     * Select the total number of triggers stored.
     * </p>
     *
     * @return the total number of triggers stored
     */
    public int selectNumTriggers() {
        return scard(keyOfTriggers());
    }

    /**
     * <p>
     * Select all of the trigger group names that are stored.
     * </p>
     *
     * @return an array of <code>String</code> group names
     */
    public List<String> selectTriggerGroups() {
        Set<String> groupWithNames = smembers(keyOfTriggers());
        Set<String> groups = new HashSet<>();
        for (String item : groupWithNames) {
            String[] array = splitValue(item);
            groups.add(array[1]);
        }
        return new LinkedList<>(groups);
    }

    public List<String> selectTriggerGroups(GroupMatcher<TriggerKey> matcher) {
        List<String> groupWithNames = sscan(keyOfTriggers(), toGroupLikeClause(matcher));
        List<String> results = new LinkedList<String>();
        for (String item : groupWithNames) {
            String[] array = splitValue(item);
            results.add(array[1]);
        }
        return results;
    }

    /**
     * <p>
     * Select all of the triggers contained in a given group.
     * </p>
     *
     * @param matcher
     *          to evaluate against known triggers
     * @return a Set of <code>TriggerKey</code>s
     */
    public Set<TriggerKey> selectTriggersInGroup(GroupMatcher<TriggerKey> matcher) {
        List<String> groupWithNames = sscan(keyOfTriggers(), toGroupLikeClause(matcher));
        Set<TriggerKey> results = new HashSet<TriggerKey>(groupWithNames.size());
        for (String item : groupWithNames) {
            String[] array = splitValue(item);
            results.add(new TriggerKey(array[0], array[1]));
        }
        return results;
    }

    public long insertPausedTriggerGroup(String groupName) {
        return sadd(keyOfPausedTriggerGroups(), groupName);
    }

    public int deletePausedTriggerGroup(String groupName) {
        return srem(keyOfPausedTriggerGroups(), groupName);
    }

    public void deletePausedTriggerGroup(GroupMatcher<TriggerKey> matcher) {
        String key = keyOfPausedTriggerGroups();
        List<String> groupNames = sscan(key, toGroupLikeClause(matcher));
        srem(key, groupNames.toArray(new String[]{}));
    }

    public boolean isTriggerGroupPaused(String groupName) {
        return sismember(keyOfPausedTriggerGroups(), groupName);
    }

    //---------------------------------------------------------------------------
    // calendars
    //---------------------------------------------------------------------------

    /**
     * <p>
     * Update a calendar.
     * </p>
     *
     * @param calendarName
     *          the name for the new calendar
     * @param calendar
     *          the calendar
     * @return the number of rows updated
     * @throws IOException
     *           if there were problems serializing the calendar
     */
    public String updateCalendar(String calendarName,
                              Calendar calendar) throws IOException {
        String calendarInfo = objectMapper.writeValueAsString(calendar);
        return set(keyOfCalendar(calendarName), calendarInfo);
    }

    /**
     * <p>
     * Update a calendar.
     * </p>
     *
     * @param calendarName
     *          the name for the new calendar
     * @param calendar
     *          the calendar
     * @return the number of rows updated
     * @throws IOException
     *           if there were problems serializing the calendar
     */
    public String insertCalendar(String calendarName,
                                 Calendar calendar) throws IOException {
        sadd(keyOfCalendars(), calendarName);
        return updateCalendar(calendarName, calendar);
    }

    /**
     * <p>
     * Check whether or not a calendar exists.
     * </p>
     *
     * @param calendarName
     *          the name of the calendar
     * @return true if the trigger exists, false otherwise
     */
    public boolean calendarExists(String calendarName) {
        return exists(keyOfCalendar(calendarName));
    }

    /**
     * <p>
     * Select a calendar.
     * </p>
     *
     * @param calendarName
     *          the name of the calendar
     * @return the Calendar
     * @throws IOException
     *           if there were problems deserializing the calendar
     */
    public Calendar selectCalendar(String calendarName)
            throws IOException {
        String calendarInfo = get(keyOfCalendar(calendarName));
        return calendarInfo == null ? null : objectMapper.readValue(calendarInfo, BaseCalendar.class);
    }

    /**
     * <p>
     * Check whether or not a calendar is referenced by any triggers.
     * </p>
     *
     * @param calendarName
     *          the name of the calendar
     * @return true if any triggers reference the calendar, false otherwise
     */
    public boolean calendarIsReferenced(String calendarName) {
        return exists(keyOfCalendarTriggers(calendarName));
    }

    /**
     * <p>
     * Delete a calendar.
     * </p>
     *
     * @param calendarName
     *          the name of the trigger
     * @return the number of rows deleted
     */
    public long deleteCalendar(String calendarName) {
        return del(calendarName);
    }

    /**
     * <p>
     * Select the total number of calendars stored.
     * </p>
     *
     * @return the total number of calendars stored
     */
    public int selectNumCalendars() {
        return scard(keyOfCalendars());
    }

    /**
     * <p>
     * Select all of the stored calendars.
     * </p>
     *
     * @return an array of <code>String</code> calendar names
     */
    public List<String> selectCalendars() {
        return new LinkedList<>(smembers(keyOfCalendars()));
    }

    //---------------------------------------------------------------------------
    // trigger firing
    //---------------------------------------------------------------------------

    /**
     * <p>
     * Select the next trigger which will fire to fire between the two given timestamps
     * in ascending order of fire time, and then descending by priority.
     * </p>
     *
     * @param noLaterThan
     *          highest value of <code>getNextFireTime()</code> of the triggers (exclusive)
     * @param noEarlierThan
     *          highest value of <code>getNextFireTime()</code> of the triggers (inclusive)
     * @param maxCount
     *          maximum number of trigger keys allow to acquired in the returning list.
     *
     * @return A (never null, possibly empty) list of the identifiers (Key objects) of the next triggers to be fired.
     */
    public List<TriggerKey> selectTriggerToAcquire(long noLaterThan, long noEarlierThan, int maxCount) {

        List<TriggerKey> nextTriggers = new LinkedList<TriggerKey>();

        // Set max rows to retrieve
        if (maxCount < 1)
            maxCount = 1; // we want at least one trigger back.
        List<String> groupWithTriggerNames = zrangebyscore(keyOfWaitingStateTriggers(), noLaterThan);
        for (int i = 0; i < groupWithTriggerNames.size(); i++) {
            if (nextTriggers.size() < maxCount) {
                String[] array = splitValue(groupWithTriggerNames.get(i));
                TriggerKey triggerKey = new TriggerKey(array[0], array[1]);
                List<KeyValue<String, String>> keyValues = hmget(keyOfTrigger(triggerKey), FIELD_MISFIRE_INSTRUCTION, FIELD_NEXT_FIRE_TIME);
                if ("-1".equals(keyValues.get(0).getValue())
                        || (keyValues.get(1).getValue() != null && Long.parseLong(keyValues.get(1).getValue()) >= noEarlierThan)) {
                    nextTriggers.add(triggerKey);
                }
            }
        }

        return nextTriggers;
    }

    /**
     * <p>
     * Insert a fired trigger.
     * </p>
     *
     * @param trigger
     *          the trigger
     * @param state
     *          the state that the trigger should be stored in
     */
    public void insertFiredTrigger(OperableTrigger trigger,
                                  String state) {
        updateFiredTrigger(trigger, state);
        sadd(keyOfFiredJobs(instanceId), trigger.getFireInstanceId());
        sadd(keyOfFiredInstances(), instanceId);
    }

    /**
     * <p>
     * Update a fired trigger.
     * </p>
     *
     * @param trigger
     *          the trigger
     * @param state
     *          the state that the trigger should be stored in
     */
    public void updateFiredTrigger(OperableTrigger trigger,
                                  String state) {
        Map<String, String> map = new HashMap<>();
        map.put(FIELD_ENTRY_ID, trigger.getFireInstanceId());
        map.put(FIELD_TRIGGER_NAME, trigger.getKey().getName());
        map.put(FIELD_TRIGGER_GROUP, trigger.getKey().getGroup());
        map.put(FIELD_INSTANCE_ID, instanceId);
        map.put(FIELD_FIRED_TIME, String.valueOf(System.currentTimeMillis()));
        map.put(FIELD_SCHED_TIME, String.valueOf(trigger.getNextFireTime().getTime()));
        map.put(FIELD_STATE, state);
        map.put(FIELD_JOB_NAME, trigger.getJobKey().getName());
        map.put(FIELD_JOB_GROUP, trigger.getJobKey().getGroup());
        map.put(FIELD_PRIORITY, String.valueOf(trigger.getPriority()));
        hmset(keyOfFiredJob(trigger.getFireInstanceId()), map);
    }

    /**
     * <p>
     * Select the states of all fired-trigger records for a given trigger, or
     * trigger group if trigger name is <code>null</code>.
     * </p>
     *
     * @return a List of FiredTriggerRecord objects.
     */
    public List<FiredTriggerRecord> selectFiredTriggerRecords(String triggerName, String groupName) {

        List<FiredTriggerRecord> lst = new LinkedList<FiredTriggerRecord>();

        Set<String> instanceNames = smembers(keyOfFiredInstances());
        for (String instanceName : instanceNames) {
            Set<String> entryIds = smembers(keyOfFiredJobs(instanceName));
            for (String entryId : entryIds) {
                Map<String, String> firedJobMap = hgetall(keyOfFiredJob(entryId));
                FiredTriggerRecordEntity recordEntity = objectMapper.convertValue(firedJobMap, FiredTriggerRecordEntity.class);
                if (groupName.equals(recordEntity.getTriggerGroup())
                        && (triggerName == null || triggerName.equals(recordEntity.getTriggerName()))) {
                    lst.add(recordEntity.trans());
                }
            }
        }

        return lst;
    }

    public List<FiredTriggerRecord> selectInstancesFiredTriggerRecords(String instanceName) {

        List<FiredTriggerRecord> lst = new LinkedList<FiredTriggerRecord>();

        Set<String> entryIds = smembers(keyOfFiredJobs(instanceName));
        for (String entryId : entryIds) {
            Map<String, String> firedJobMap = hgetall(keyOfFiredJob(entryId));
            FiredTriggerRecordEntity recordEntity = objectMapper.convertValue(firedJobMap, FiredTriggerRecordEntity.class);
            lst.add(recordEntity.trans());
        }

        return lst;
    }

    /**
     * <p>
     * Select the distinct instance names of all fired-trigger records.
     * </p>
     *
     * <p>
     * This is useful when trying to identify orphaned fired triggers (a
     * fired trigger without a scheduler state record.)
     * </p>
     *
     * @return a Set of String objects.
     */
    public Set<String> selectFiredTriggerInstanceNames() {
        return smembers(keyOfFiredInstances());
    }

    /**
     * <p>
     * Delete a fired trigger.
     * </p>
     *
     * @param entryId
     *          the fired trigger entry to delete
     * @return the number of rows deleted
     */
    public void deleteFiredTrigger(String entryId) {
        del(keyOfFiredJob(entryId));
        Set<String> firedInstanceNames = smembers(keyOfFiredInstances());
        for (String instanceName : firedInstanceNames) {
            srem(keyOfFiredJobs(instanceName), entryId);  // if exits
        }
    }

    public int insertSchedulerState(String theInstanceId,
                                    long checkInTime, long interval) {
        long res = sadd(keyOfSchedulerStates(), theInstanceId);
        Map<String, String> map = new HashMap<>();
        map.put(FIELD_LAST_CHECKIN_TIME, String.valueOf(checkInTime));
        map.put(FIELD_CHECKIN_INTERVAL, String.valueOf(interval));
        hmset(keyOfSchedulerState(theInstanceId), map);
        return (int) res;
    }

    public int deleteSchedulerState(String theInstanceId) {
        srem(keyOfSchedulerStates(), theInstanceId);
        return del(keyOfSchedulerState(theInstanceId));
    }

    public int updateSchedulerState(String theInstanceId, long checkInTime) {
        return hset(keyOfSchedulerState(theInstanceId), FIELD_LAST_CHECKIN_TIME, String.valueOf(checkInTime)) ? 1 : 0;
    }

    public List<SchedulerStateRecord> selectSchedulerStateRecords() {
        List<SchedulerStateRecord> lst = new LinkedList<SchedulerStateRecord>();

        Set<String> instanceIds = smembers(keyOfSchedulerStates());
        for (String instanceId : instanceIds) {
            SchedulerStateRecord rec = new SchedulerStateRecord();
            Map<String, String> map = hgetall(keyOfSchedulerState(instanceId));

            rec.setSchedulerInstanceId(instanceId);
            rec.setCheckinTimestamp(Long.parseLong(map.get(FIELD_LAST_CHECKIN_TIME)));
            rec.setCheckinInterval(Long.parseLong(map.get(FIELD_CHECKIN_INTERVAL)));

            lst.add(rec);
        }
        return lst;
    }

    //---------------------------------------------------------------------------
    // other
    //---------------------------------------------------------------------------

    public Set<String> selectPausedTriggerGroups() {
        return smembers(keyOfPausedTriggerGroups());
    }

}
