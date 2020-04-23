package com.github.quartz.impl.redisjobstore;

import com.zaxxer.hikari.HikariDataSource;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.UUID;

/**
 * @author ldang
 */
public class HelloJob implements Job {

    public static HikariDataSource dataSource = Util.getDataSource();

    private static final String INSERT_INSTANCE = "insert into instance(time, name) values (?, ?)";

    private static final String INSERT_LOG = "insert into log(id, time, name, start) values (?, ?, ?, ?)";

    private static final String UPDATE_LOG = "update log set end = ? where id = ?";

    private static final String yyyyMMddHHmmssSSS = "yyyy-MM-dd HH:mm:ss SSS";

    private static final String yyyyMMddHHmmss = "yyyy-MM-dd HH:mm:ss";

    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
        Date now = new Date();
        String name = context.getJobDetail().getKey().getName();
        String time = new SimpleDateFormat(yyyyMMddHHmmss).format(now);
        String start =  new SimpleDateFormat(yyyyMMddHHmmssSSS).format(now);
        System.out.println(start + "    " + context.getJobDetail().getKey().getName() + "    开始执行");
        Connection connection = null;
        try {
            String id = UUID.randomUUID().toString();
            connection = dataSource.getConnection();
            PreparedStatement ps1 = connection.prepareStatement(INSERT_LOG);
            ps1.setString(1, id);
            ps1.setString(2, time);
            ps1.setString(3, name);
            ps1.setString(4, start);
            //ps1.setString(5, end);
            ps1.execute();

            PreparedStatement ps2 = connection.prepareStatement(INSERT_INSTANCE);
            ps2.setString(1, time);
            ps2.setString(2, name);
            ps2.execute();

            //Thread.sleep(100L);
            Date endDate = new Date();
            String end =  new SimpleDateFormat(yyyyMMddHHmmssSSS).format(endDate);
            PreparedStatement ps3 = connection.prepareStatement(UPDATE_LOG);
            ps3.setString(1, end);
            ps3.setString(2, id);
            ps3.execute();
            System.out.println(new SimpleDateFormat(yyyyMMddHHmmssSSS).format(new Date()) + "    " + context.getJobDetail().getKey().getName() + "    结束执行");
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }

    }
}
