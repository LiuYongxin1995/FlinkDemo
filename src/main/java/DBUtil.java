import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class DBUtil {

    public static List<AlarmRule> queryAlarmTable() {
        Properties props = PropUtil.loadProperties("/mysql.properties");
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        List<AlarmRule> alarmRuleList = new ArrayList<AlarmRule>();
        try {
            Class.forName(props.getProperty("drivername"));
            connection = DriverManager.getConnection(props.getProperty("dburl"),
                    props.getProperty("username"), props.getProperty("password"));
            String sql = "select keyword,level,description,filename from dmp_alarm_rule_info";
            preparedStatement = connection.prepareStatement(sql);
            resultSet = preparedStatement.executeQuery();
            while (resultSet.next()) {
                String keyWord = resultSet.getString("keyword");
                String level = resultSet.getString("level");
                String description = resultSet.getString("description");
                String filename = resultSet.getString("filename");
                AlarmRule alarmRule = new AlarmRule();
                alarmRule.setKeyword(keyWord);
                alarmRule.setLevel(level);
                alarmRule.setDescription(description);
                alarmRule.setFilename(filename);
                alarmRuleList.add(alarmRule);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (preparedStatement != null) {
                try {
                    preparedStatement.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (resultSet != null) {
                try {
                    resultSet.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }

        return alarmRuleList;
    }
}
