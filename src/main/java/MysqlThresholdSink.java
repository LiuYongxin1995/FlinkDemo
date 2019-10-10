import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.UUID;

public class MysqlThresholdSink extends RichSinkFunction<Tuple3<String, Long, Long>> {

    public static SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private Connection connection;
    private PreparedStatement preparedStatement;
    String username = "icomp";
    String password = "icomp";
    String drivername = "com.mysql.jdbc.Driver";
    String dburl = "jdbc:mysql://10.10.49.24:3306/imp?useOldAliasMetadataBehavior=true&amp;useUnicode=true&amp;characterEncoding=utf-8&amp;allowMultiQueries=true&amp;autoReconnect=true&amp;maxReconnects=3";

    public void invoke(Tuple3<String, Long, Long> value) throws Exception {
        Class.forName(drivername);
        connection = DriverManager.getConnection(dburl, username, password);
        String sql = "insert into imp_events(EVENT_ID,EVENT_TYPE,LEVEL,SUMMARYCN,TIME,DEVICE_IP,PROCESS_STATUS,EVENT_STATUS,LAST_TIME,EVENT_TYPE_ALL) values(?,?,?,?,?,?,?,?,?,?)";
        List<AlarmRule> ruleList = DBUtil.queryAlarmTable();
        for (AlarmRule alarmRule : ruleList) {
            String keyword = alarmRule.getKeyword();
            //String threshold = alarmRule.getThreshold();
            String threshold="";
            if (value.f0.equals(keyword) && value.f2 > Long.parseLong(threshold)) {
                String eventid = UUID.randomUUID().toString();
                String eventtype = "LOG.APP";
                String level = alarmRule.getLevel();
                String content = alarmRule.getDescription();
                Date date = new Date();
                String time = format.format(date.getTime());
                String deviceIp = "";
                String processstatus = "0";
                String eventstatus = "1";
                String lasttime = time;
                String eventtypeall = "LOG";
                preparedStatement = connection.prepareStatement(sql);
                preparedStatement.setString(1, eventid);
                preparedStatement.setString(2, eventtype);
                preparedStatement.setString(3, level);
                preparedStatement.setString(4, content);
                preparedStatement.setString(5, time);
                preparedStatement.setString(6, deviceIp);
                preparedStatement.setString(7, processstatus);
                preparedStatement.setString(8, eventstatus);
                preparedStatement.setString(9, lasttime);
                preparedStatement.setString(10, eventtypeall);
                preparedStatement.executeUpdate();
                if (preparedStatement != null) {
                    preparedStatement.close();
                }
                if (connection != null) {
                    connection.close();
                }
            }
        }
    }
}
