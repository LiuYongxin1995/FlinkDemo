import org.apache.flink.api.java.tuple.Tuple16;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Properties;

public class MysqlSink extends RichSinkFunction<Tuple16<String, String, String, String, String, String, String, String, String, String, String, String, String, String, Long, Long>> {

    private Connection connection;
    private PreparedStatement preparedStatement;

    public void invoke(Tuple16<String, String, String, String, String, String, String, String, String, String, String, String, String, String, Long, Long> value) throws Exception {
        if (value.f10.equals("0")) {
            Properties props = PropUtil.loadProperties("/mysql.properties");
            Class.forName(props.getProperty("drivername"));
            connection = DriverManager.getConnection(props.getProperty("dburl"),
                    props.getProperty("username"), props.getProperty("password"));
            String sql = "insert into imp_events(EVENT_ID,EVENT_TYPE,LEVEL,SUMMARYCN,TIME,DEVICE_IP,PROCESS_STATUS,EVENT_STATUS,LAST_TIME,EVENT_TYPE_ALL) values(?,?,?,?,?,?,?,?,?,?)"; //假设mysql 有3列 id,num,price
            preparedStatement = connection.prepareStatement(sql);
            preparedStatement.setString(1, value.f0);
            preparedStatement.setString(2, value.f1);
            preparedStatement.setString(3, value.f2);
            preparedStatement.setString(4, value.f3);
            preparedStatement.setString(5, value.f4);
            preparedStatement.setString(6, value.f5);
            preparedStatement.setString(7, value.f6);
            preparedStatement.setString(8, value.f7);
            preparedStatement.setString(9, value.f8);
            preparedStatement.setString(10, value.f9);

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
